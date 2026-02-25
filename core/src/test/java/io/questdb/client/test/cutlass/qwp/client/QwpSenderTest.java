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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.test.cutlass.line.AbstractLineSenderTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Integration tests for the QWP (QuestDB Wire Protocol) WebSocket sender.
 * <p>
 * Tests verify that all QWP native types arrive correctly (exact type match)
 * and that reasonable type coercions work (e.g., client sends INT but server
 * column is LONG).
 * <p>
 * Tests are skipped if no QuestDB instance is running
 * ({@code -Dquestdb.running=true}).
 */
public class QwpSenderTest extends AbstractLineSenderTest {

    @BeforeClass
    public static void setUpStatic() {
        AbstractLineSenderTest.setUpStatic();
    }

    @Test
    public void testBoolToString() throws Exception {
        String table = "test_qwp_bool_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("s", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .boolColumn("s", false)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "true\t1970-01-01T00:00:01.000000000Z\n" +
                        "false\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testBoolToVarchar() throws Exception {
        String table = "test_qwp_bool_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .boolColumn("v", false)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "true\t1970-01-01T00:00:01.000000000Z\n" +
                        "false\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testBoolean() throws Exception {
        String table = "test_qwp_boolean";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("b", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .boolColumn("b", false)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "b\ttimestamp\n" +
                        "true\t1970-01-01T00:00:01.000000000Z\n" +
                        "false\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT b, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testBooleanToByteCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("BYTE")
            );
        }
    }

    @Test
    public void testBooleanToCharCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testBooleanToDateCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("DATE")
            );
        }
    }

    @Test
    public void testBooleanToDecimalCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_decimal_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("DECIMAL")
            );
        }
    }

    @Test
    public void testBooleanToDoubleCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("DOUBLE")
            );
        }
    }

    @Test
    public void testBooleanToFloatCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("FLOAT")
            );
        }
    }

    @Test
    public void testBooleanToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("GEOHASH")
            );
        }
    }

    @Test
    public void testBooleanToIntCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testBooleanToLong256CoercionError() throws Exception {
        String table = "test_qwp_boolean_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("LONG256")
            );
        }
    }

    @Test
    public void testBooleanToLongCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("LONG")
            );
        }
    }

    @Test
    public void testBooleanToShortCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("SHORT")
            );
        }
    }

    @Test
    public void testBooleanToSymbolCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testBooleanToTimestampCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_timestamp_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testBooleanToTimestampNsCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_timestamp_ns_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testBooleanToUuidCoercionError() throws Exception {
        String table = "test_qwp_boolean_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .boolColumn("v", true)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write BOOLEAN") && msg.contains("UUID")
            );
        }
    }

    @Test
    public void testByte() throws Exception {
        String table = "test_qwp_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("b", (short) -1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("b", (short) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("b", (short) 127)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
    }

    @Test
    public void testByteToBooleanCoercionError() throws Exception {
        String table = "test_qwp_byte_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("b", (byte) 1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning BYTE and BOOLEAN but got: " + msg,
                    msg.contains("BYTE") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testByteToCharCoercionError() throws Exception {
        String table = "test_qwp_byte_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "c CHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("c", (byte) 65)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning BYTE and CHAR but got: " + msg,
                    msg.contains("BYTE") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testByteToDate() throws Exception {
        String table = "test_qwp_byte_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DATE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 100)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1970-01-01T00:00:00.100000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal() throws Exception {
        String table = "test_qwp_byte_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(6, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal128() throws Exception {
        String table = "test_qwp_byte_to_decimal128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal16() throws Exception {
        String table = "test_qwp_byte_to_decimal16";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(4, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -9)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal256() throws Exception {
        String table = "test_qwp_byte_to_decimal256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal64() throws Exception {
        String table = "test_qwp_byte_to_decimal64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDecimal8() throws Exception {
        String table = "test_qwp_byte_to_decimal8";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -9)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "5.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToDouble() throws Exception {
        String table = "test_qwp_byte_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("d", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("d", (byte) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToFloat() throws Exception {
        String table = "test_qwp_byte_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("f", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("f", (byte) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "f\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_byte_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "g GEOHASH(4c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("g", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error mentioning BYTE but got: " + msg,
                    msg.contains("type coercion from BYTE to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testByteToInt() throws Exception {
        String table = "test_qwp_byte_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("i", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("i", Byte.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("i", Byte.MIN_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "i\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "127\t1970-01-01T00:00:02.000000000Z\n" +
                        "-128\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToLong() throws Exception {
        String table = "test_qwp_byte_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("l", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("l", Byte.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("l", Byte.MIN_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "l\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "127\t1970-01-01T00:00:02.000000000Z\n" +
                        "-128\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToLong256CoercionError() throws Exception {
        String table = "test_qwp_byte_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v LONG256, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("v", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from BYTE to LONG256 is not supported")
            );
        }
    }

    @Test
    public void testByteToShort() throws Exception {
        String table = "test_qwp_byte_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("s", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", Byte.MIN_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", Byte.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-128\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToString() throws Exception {
        String table = "test_qwp_byte_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("s", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", (byte) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", (byte) 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToSymbol() throws Exception {
        String table = "test_qwp_byte_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("s", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", (byte) -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("s", (byte) 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToTimestamp() throws Exception {
        String table = "test_qwp_byte_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "t TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("t", (byte) 100)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("t", (byte) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "1970-01-01T00:00:00.000100000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testByteToUuidCoercionError() throws Exception {
        String table = "test_qwp_byte_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "u UUID, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("u", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from BYTE to UUID is not supported")
            );
        }
    }

    @Test
    public void testByteToVarchar() throws Exception {
        String table = "test_qwp_byte_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .byteColumn("v", (byte) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("v", (byte) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .byteColumn("v", Byte.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "v\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testChar() throws Exception {
        String table = "test_qwp_char";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("c", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .charColumn("c", 'ü') // ü
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .charColumn("c", '中') // 中
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "c\ttimestamp\n" +
                        "A\t1970-01-01T00:00:01.000000000Z\n" +
                        "ü\t1970-01-01T00:00:02.000000000Z\n" +
                        "中\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT c, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testCharToBooleanCoercionError() throws Exception {
        String table = "test_qwp_char_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testCharToByteCoercionError() throws Exception {
        String table = "test_qwp_char_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("BYTE")
            );
        }
    }

    @Test
    public void testCharToDateCoercionError() throws Exception {
        String table = "test_qwp_char_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("DATE")
            );
        }
    }

    @Test
    public void testCharToDoubleCoercionError() throws Exception {
        String table = "test_qwp_char_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("DOUBLE")
            );
        }
    }

    @Test
    public void testCharToFloatCoercionError() throws Exception {
        String table = "test_qwp_char_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("FLOAT")
            );
        }
    }

    @Test
    public void testCharToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_char_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("GEOHASH")
            );
        }
    }

    @Test
    public void testCharToIntCoercionError() throws Exception {
        String table = "test_qwp_char_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testCharToLong256CoercionError() throws Exception {
        String table = "test_qwp_char_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("LONG256")
            );
        }
    }

    @Test
    public void testCharToLongCoercionError() throws Exception {
        String table = "test_qwp_char_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("LONG")
            );
        }
    }

    @Test
    public void testCharToShortCoercionError() throws Exception {
        String table = "test_qwp_char_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("SHORT")
            );
        }
    }

    @Test
    public void testCharToString() throws Exception {
        String table = "test_qwp_char_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("s", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .charColumn("s", 'Z')
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "A\t1970-01-01T00:00:01.000000000Z\n" +
                        "Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testCharToSymbolCoercionError() throws Exception {
        String table = "test_qwp_char_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testCharToUuidCoercionError() throws Exception {
        String table = "test_qwp_char_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("not supported") && msg.contains("UUID")
            );
        }
    }

    @Test
    public void testCharToVarchar() throws Exception {
        String table = "test_qwp_char_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .charColumn("v", 'A')
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .charColumn("v", 'Z')
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "A\t1970-01-01T00:00:01.000000000Z\n" +
                        "Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal() throws Exception {
        String table = "test_qwp_decimal";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("d", "123.45")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", "-999.99")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", "0.01")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(42_000, 2))
                    .at(4_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 4);
    }

    @Test
    public void testDecimal128ToDecimal256() throws Exception {
        String table = "test_qwp_dec128_to_dec256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("d", Decimal128.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal128.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal128ToDecimal64() throws Exception {
        String table = "test_qwp_dec128_to_dec64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("d", Decimal128.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal128.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal256ToDecimal128() throws Exception {
        String table = "test_qwp_dec256_to_dec128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal256ToDecimal64() throws Exception {
        String table = "test_qwp_dec256_to_dec64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Send DECIMAL256 wire type to DECIMAL64 column
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal256ToDecimal64OverflowError() throws Exception {
        String table = "test_qwp_dec256_to_dec64_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Create a value that fits in Decimal256 but overflows Decimal64
            // Decimal256 with hi bits set will overflow 64-bit storage
            Decimal256 bigValue = Decimal256.fromBigDecimal(new java.math.BigDecimal("99999999999999999999.99"));
            sender.table(table)
                    .decimalColumn("d", bigValue)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("decimal value overflows")
            );
        }
    }

    @Test
    public void testDecimal256ToDecimal8OverflowError() throws Exception {
        String table = "test_qwp_dec256_to_dec8_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // 999.9 with scale=1 → unscaled 9999, which doesn't fit in a byte (-128..127)
            sender.table(table)
                    .decimalColumn("d", Decimal256.fromLong(9999, 1))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("decimal value overflows")
            );
        }
    }

    @Test
    public void testDecimal64ToDecimal128() throws Exception {
        String table = "test_qwp_dec64_to_dec128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Send DECIMAL64 wire type to DECIMAL128 column (widening)
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimal64ToDecimal256() throws Exception {
        String table = "test_qwp_dec64_to_dec256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimalRescale() throws Exception {
        String table = "test_qwp_decimal_rescale";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 4), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Send scale=2 wire data to scale=4 column: server should rescale
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("d", Decimal64.fromLong(-100, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.4500\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1.0000\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimalToBooleanCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testDecimalToByteCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("BYTE")
            );
        }
    }

    @Test
    public void testDecimalToCharCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testDecimalToDateCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("DATE")
            );
        }
    }

    @Test
    public void testDecimalToDoubleCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("DOUBLE")
            );
        }
    }

    @Test
    public void testDecimalToFloatCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("FLOAT")
            );
        }
    }

    @Test
    public void testDecimalToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("GEOHASH")
            );
        }
    }

    @Test
    public void testDecimalToIntCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testDecimalToLong256CoercionError() throws Exception {
        String table = "test_qwp_decimal_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("LONG256")
            );
        }
    }

    @Test
    public void testDecimalToLongCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("LONG")
            );
        }
    }

    @Test
    public void testDecimalToShortCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("SHORT")
            );
        }
    }

    @Test
    public void testDecimalToString() throws Exception {
        String table = "test_qwp_decimal_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("s", Decimal64.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("s", Decimal64.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDecimalToSymbolCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testDecimalToTimestampCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_timestamp_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testDecimalToTimestampNsCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_timestamp_ns_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testDecimalToUuidCoercionError() throws Exception {
        String table = "test_qwp_decimal_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DECIMAL64") && msg.contains("UUID")
            );
        }
    }

    @Test
    public void testDecimalToVarchar() throws Exception {
        String table = "test_qwp_decimal_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(12345, 2))
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .decimalColumn("v", Decimal64.fromLong(-9999, 2))
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDouble() throws Exception {
        String table = "test_qwp_double";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("d", 42.5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", -1.0E10)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", Double.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", Double.MIN_VALUE)
                    .at(4_000_000, ChronoUnit.MICROS);
            // NaN and Inf should be stored as null
            sender.table(table)
                    .doubleColumn("d", Double.NaN)
                    .at(5_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", Double.POSITIVE_INFINITY)
                    .at(6_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", Double.NEGATIVE_INFINITY)
                    .at(7_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 7);
        assertSqlEventually(
                "d\ttimestamp\n" +
                        "42.5\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1.0E10\t1970-01-01T00:00:02.000000000Z\n" +
                        "1.7976931348623157E308\t1970-01-01T00:00:03.000000000Z\n" +
                        "4.9E-324\t1970-01-01T00:00:04.000000000Z\n" +
                        "null\t1970-01-01T00:00:05.000000000Z\n" +
                        "null\t1970-01-01T00:00:06.000000000Z\n" +
                        "null\t1970-01-01T00:00:07.000000000Z\n",
                "SELECT d, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testDoubleArray() throws Exception {
        String table = "test_qwp_double_array";
        useTable(table);

        double[] arr1d = createDoubleArray(5);
        double[][] arr2d = createDoubleArray(2, 3);
        double[][][] arr3d = createDoubleArray(1, 2, 3);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleArray("a1", arr1d)
                    .doubleArray("a2", arr2d)
                    .doubleArray("a3", arr3d)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
    }

    @Test
    public void testDoubleArrayToIntCoercionError() throws Exception {
        String table = "test_qwp_doublearray_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleArray("v", new double[]{1.0, 2.0})
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testDoubleArrayToStringCoercionError() throws Exception {
        String table = "test_qwp_doublearray_to_string_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleArray("v", new double[]{1.0, 2.0})
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("STRING")
            );
        }
    }

    @Test
    public void testDoubleArrayToSymbolCoercionError() throws Exception {
        String table = "test_qwp_doublearray_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleArray("v", new double[]{1.0, 2.0})
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testDoubleArrayToTimestampCoercionError() throws Exception {
        String table = "test_qwp_doublearray_to_timestamp_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleArray("v", new double[]{1.0, 2.0})
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testDoubleToBooleanCoercionError() throws Exception {
        String table = "test_qwp_double_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testDoubleToByte() throws Exception {
        String table = "test_qwp_double_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("b", 42.0)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("b", -100.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToByteOverflowError() throws Exception {
        String table = "test_qwp_double_to_byte_ovf";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("b", 200.0)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 200 out of range for BYTE")
            );
        }
    }

    @Test
    public void testDoubleToBytePrecisionLossError() throws Exception {
        String table = "test_qwp_double_to_byte_prec";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("b", 42.5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected precision loss error but got: " + msg,
                    msg.contains("loses precision") && msg.contains("42.5")
            );
        }
    }

    @Test
    public void testDoubleToCharCoercionError() throws Exception {
        String table = "test_qwp_double_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write DOUBLE") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testDoubleToDateCoercionError() throws Exception {
        String table = "test_qwp_double_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testDoubleToDecimal() throws Exception {
        String table = "test_qwp_double_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(10, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("d", 123.45)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("d", -42.10)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42.10\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToDecimalPrecisionLossError() throws Exception {
        String table = "test_qwp_double_to_decimal_prec";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(10, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("d", 123.456)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected precision loss error but got: " + msg,
                    msg.contains("cannot be converted to") && msg.contains("123.456") && msg.contains("scale=2")
            );
        }
    }

    @Test
    public void testDoubleToFloat() throws Exception {
        String table = "test_qwp_double_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("f", 1.5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("f", -42.25)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
    }

    @Test
    public void testDoubleToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_double_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testDoubleToInt() throws Exception {
        String table = "test_qwp_double_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("i", 100_000.0)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("i", -42.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "i\tts\n" +
                        "100000\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToIntPrecisionLossError() throws Exception {
        String table = "test_qwp_double_to_int_prec";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("i", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected precision loss error but got: " + msg,
                    msg.contains("loses precision") && msg.contains("3.14")
            );
        }
    }

    @Test
    public void testDoubleToLong() throws Exception {
        String table = "test_qwp_double_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("l", 1_000_000.0)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("l", -42.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "l\tts\n" +
                        "1000000\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToLong256CoercionError() throws Exception {
        String table = "test_qwp_double_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testDoubleToShort() throws Exception {
        String table = "test_qwp_double_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 100.0)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("v", -200.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "100\t1970-01-01T00:00:01.000000000Z\n" +
                        "-200\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToString() throws Exception {
        String table = "test_qwp_double_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("s", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("s", -42.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToSymbol() throws Exception {
        String table = "test_qwp_double_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "sym SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("sym", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "sym\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT sym, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testDoubleToUuidCoercionError() throws Exception {
        String table = "test_qwp_double_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testDoubleToVarchar() throws Exception {
        String table = "test_qwp_double_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .doubleColumn("v", 3.14)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .doubleColumn("v", -42.0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloat() throws Exception {
        String table = "test_qwp_float";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("f", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("f", -42.25f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("f", 0.0f)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
    }

    @Test
    public void testFloatToBooleanCoercionError() throws Exception {
        String table = "test_qwp_float_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write FLOAT") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testFloatToByte() throws Exception {
        String table = "test_qwp_float_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 7.0f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("v", -100.0f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "7\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToCharCoercionError() throws Exception {
        String table = "test_qwp_float_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write FLOAT") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testFloatToDateCoercionError() throws Exception {
        String table = "test_qwp_float_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testFloatToDecimal() throws Exception {
        String table = "test_qwp_float_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(10, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("d", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("d", -42.25f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1.50\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42.25\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToDecimalPrecisionLossError() throws Exception {
        String table = "test_qwp_float_to_decimal_prec";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(10, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("d", 1.25f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected precision loss error but got: " + msg,
                    msg.contains("cannot be converted to") && msg.contains("scale=1")
            );
        }
    }

    @Test
    public void testFloatToDouble() throws Exception {
        String table = "test_qwp_float_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("d", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("d", -42.25f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1.5\t1970-01-01T00:00:01.000000000Z\n" +
                        "-42.25\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_float_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testFloatToInt() throws Exception {
        String table = "test_qwp_float_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("i", 42.0f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("i", -100.0f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "i\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToIntPrecisionLossError() throws Exception {
        String table = "test_qwp_float_to_int_prec";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("i", 3.14f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected precision loss error but got: " + msg,
                    msg.contains("loses precision")
            );
        }
    }

    @Test
    public void testFloatToLong() throws Exception {
        String table = "test_qwp_float_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("l", 1000.0f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "l\tts\n" +
                        "1000\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToLong256CoercionError() throws Exception {
        String table = "test_qwp_float_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testFloatToShort() throws Exception {
        String table = "test_qwp_float_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 42.0f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .floatColumn("v", -1000.0f)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1000\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToString() throws Exception {
        String table = "test_qwp_float_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("s", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "s\tts\n" +
                        "1.5\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToSymbol() throws Exception {
        String table = "test_qwp_float_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "sym SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("sym", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "sym\tts\n" +
                        "1.5\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT sym, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testFloatToUuidCoercionError() throws Exception {
        String table = "test_qwp_float_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testFloatToVarchar() throws Exception {
        String table = "test_qwp_float_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .floatColumn("v", 1.5f)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "v\tts\n" +
                        "1.5\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testInt() throws Exception {
        String table = "test_qwp_int";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Integer.MIN_VALUE is the null sentinel for INT
            sender.table(table)
                    .intColumn("i", Integer.MIN_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("i", 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("i", Integer.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("i", -42)
                    .at(4_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 4);
        assertSqlEventually(
                "i\ttimestamp\n" +
                        "null\t1970-01-01T00:00:01.000000000Z\n" +
                        "0\t1970-01-01T00:00:02.000000000Z\n" +
                        "2147483647\t1970-01-01T00:00:03.000000000Z\n" +
                        "-42\t1970-01-01T00:00:04.000000000Z\n",
                "SELECT i, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testIntToBooleanCoercionError() throws Exception {
        String table = "test_qwp_int_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("b", 1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning INT and BOOLEAN but got: " + msg,
                    msg.contains("INT") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testIntToByte() throws Exception {
        String table = "test_qwp_int_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("b", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("b", -128)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("b", 127)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-128\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToByteOverflowError() throws Exception {
        String table = "test_qwp_int_to_byte_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("b", 128)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 128 out of range for BYTE")
            );
        }
    }

    @Test
    public void testIntToCharCoercionError() throws Exception {
        String table = "test_qwp_int_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "c CHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("c", 65)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning INT and CHAR but got: " + msg,
                    msg.contains("INT") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testIntToDate() throws Exception {
        String table = "test_qwp_int_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DATE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // 86_400_000 millis = 1 day
            sender.table(table)
                    .intColumn("d", 86_400_000)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1970-01-02T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal() throws Exception {
        String table = "test_qwp_int_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(6, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal128() throws Exception {
        String table = "test_qwp_int_to_decimal128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.00\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal16() throws Exception {
        String table = "test_qwp_int_to_decimal16";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(4, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal256() throws Exception {
        String table = "test_qwp_int_to_decimal256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.00\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal64() throws Exception {
        String table = "test_qwp_int_to_decimal64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", Integer.MAX_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "d\tts\n" +
                        "2147483647.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.00\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDecimal8() throws Exception {
        String table = "test_qwp_int_to_decimal8";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -9)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "d\tts\n" +
                        "5.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.0\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToDouble() throws Exception {
        String table = "test_qwp_int_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToFloat() throws Exception {
        String table = "test_qwp_int_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("f", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("f", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("f", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "f\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n" +
                        "0.0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_int_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "g GEOHASH(4c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("g", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error mentioning INT but got: " + msg,
                    msg.contains("type coercion from INT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testIntToLong() throws Exception {
        String table = "test_qwp_int_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("l", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("l", Integer.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("l", -1)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "l\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "2147483647\t1970-01-01T00:00:02.000000000Z\n" +
                        "-1\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToLong256CoercionError() throws Exception {
        String table = "test_qwp_int_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v LONG256, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("v", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from INT to LONG256 is not supported")
            );
        }
    }

    @Test
    public void testIntToShort() throws Exception {
        String table = "test_qwp_int_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("s", 1000)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", -32768)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", 32767)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "1000\t1970-01-01T00:00:01.000000000Z\n" +
                        "-32768\t1970-01-01T00:00:02.000000000Z\n" +
                        "32767\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToShortOverflowError() throws Exception {
        String table = "test_qwp_int_to_short_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("s", 32768)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 32768 out of range for SHORT")
            );
        }
    }

    @Test
    public void testIntToString() throws Exception {
        String table = "test_qwp_int_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("s", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToSymbol() throws Exception {
        String table = "test_qwp_int_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("s", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("s", 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToTimestamp() throws Exception {
        String table = "test_qwp_int_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "t TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // 1_000_000 micros = 1 second
            sender.table(table)
                    .intColumn("t", 1_000_000)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("t", 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "1970-01-01T00:00:01.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testIntToUuidCoercionError() throws Exception {
        String table = "test_qwp_int_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "u UUID, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("u", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from INT to UUID is not supported")
            );
        }
    }

    @Test
    public void testIntToVarchar() throws Exception {
        String table = "test_qwp_int_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .intColumn("v", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("v", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .intColumn("v", Integer.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "v\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "2147483647\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLong() throws Exception {
        String table = "test_qwp_long";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Long.MIN_VALUE is the null sentinel for LONG
            sender.table(table)
                    .longColumn("l", Long.MIN_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("l", 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("l", Long.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "l\ttimestamp\n" +
                        "null\t1970-01-01T00:00:01.000000000Z\n" +
                        "0\t1970-01-01T00:00:02.000000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT l, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testLong256() throws Exception {
        String table = "test_qwp_long256";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // All zeros
            sender.table(table)
                    .long256Column("v", 0, 0, 0, 0)
                    .at(1_000_000, ChronoUnit.MICROS);
            // Mixed values
            sender.table(table)
                    .long256Column("v", 1, 2, 3, 4)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
    }

    @Test
    public void testLong256ToBooleanCoercionError() throws Exception {
        String table = "test_qwp_long256_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write LONG256") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testLong256ToByteCoercionError() throws Exception {
        String table = "test_qwp_long256_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToCharCoercionError() throws Exception {
        String table = "test_qwp_long256_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write LONG256") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testLong256ToDateCoercionError() throws Exception {
        String table = "test_qwp_long256_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToDoubleCoercionError() throws Exception {
        String table = "test_qwp_long256_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToFloatCoercionError() throws Exception {
        String table = "test_qwp_long256_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_long256_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToIntCoercionError() throws Exception {
        String table = "test_qwp_long256_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToLongCoercionError() throws Exception {
        String table = "test_qwp_long256_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToShortCoercionError() throws Exception {
        String table = "test_qwp_long256_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToString() throws Exception {
        String table = "test_qwp_long256_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("s", 1, 2, 3, 4)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "s\tts\n" +
                        "0x04000000000000000300000000000000020000000000000001\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT s, ts FROM " + table);
    }

    @Test
    public void testLong256ToSymbolCoercionError() throws Exception {
        String table = "test_qwp_long256_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write LONG256") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testLong256ToUuidCoercionError() throws Exception {
        String table = "test_qwp_long256_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1L, 0L, 0L, 0L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLong256ToVarchar() throws Exception {
        String table = "test_qwp_long256_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .long256Column("v", 1, 2, 3, 4)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "v\tts\n" +
                        "0x04000000000000000300000000000000020000000000000001\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT v, ts FROM " + table);
    }

    @Test
    public void testLongToBooleanCoercionError() throws Exception {
        String table = "test_qwp_long_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("b", 1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning LONG and BOOLEAN but got: " + msg,
                    msg.contains("LONG") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testLongToByte() throws Exception {
        String table = "test_qwp_long_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("b", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("b", -128)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("b", 127)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-128\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToByteOverflowError() throws Exception {
        String table = "test_qwp_long_to_byte_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("b", 128)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 128 out of range for BYTE")
            );
        }
    }

    @Test
    public void testLongToCharCoercionError() throws Exception {
        String table = "test_qwp_long_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "c CHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("c", 65)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning LONG and CHAR but got: " + msg,
                    msg.contains("LONG") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testLongToDate() throws Exception {
        String table = "test_qwp_long_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DATE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 86_400_000L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", 0L)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1970-01-02T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal() throws Exception {
        String table = "test_qwp_long_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(10, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal128() throws Exception {
        String table = "test_qwp_long_to_decimal128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 1_000_000_000L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -1_000_000_000L)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1000000000.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1000000000.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal16() throws Exception {
        String table = "test_qwp_long_to_decimal16";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(4, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal256() throws Exception {
        String table = "test_qwp_long_to_decimal256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", Long.MAX_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -1_000_000_000_000L)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "9223372036854775807.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1000000000000.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal32() throws Exception {
        String table = "test_qwp_long_to_decimal32";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(6, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDecimal8() throws Exception {
        String table = "test_qwp_long_to_decimal8";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -9)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "5.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToDouble() throws Exception {
        String table = "test_qwp_long_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("d", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("d", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToFloat() throws Exception {
        String table = "test_qwp_long_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("f", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("f", -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "f\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_long_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "g GEOHASH(4c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("g", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error mentioning LONG but got: " + msg,
                    msg.contains("type coercion from LONG to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testLongToInt() throws Exception {
        String table = "test_qwp_long_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Value in INT range should succeed
            sender.table(table)
                    .longColumn("i", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("i", -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "i\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToIntOverflowError() throws Exception {
        String table = "test_qwp_long_to_int_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("i", (long) Integer.MAX_VALUE + 1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 2147483648 out of range for INT")
            );
        }
    }

    @Test
    public void testLongToLong256CoercionError() throws Exception {
        String table = "test_qwp_long_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v LONG256, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("v", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG to LONG256 is not supported")
            );
        }
    }

    @Test
    public void testLongToShort() throws Exception {
        String table = "test_qwp_long_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Value in SHORT range should succeed
            sender.table(table)
                    .longColumn("s", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("s", -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
    }

    @Test
    public void testLongToShortOverflowError() throws Exception {
        String table = "test_qwp_long_to_short_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("s", 32768)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 32768 out of range for SHORT")
            );
        }
    }

    @Test
    public void testLongToString() throws Exception {
        String table = "test_qwp_long_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("s", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("s", Long.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToSymbol() throws Exception {
        String table = "test_qwp_long_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("s", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("s", -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToTimestamp() throws Exception {
        String table = "test_qwp_long_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "t TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("t", 1_000_000L)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("t", 0L)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "1970-01-01T00:00:01.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testLongToUuidCoercionError() throws Exception {
        String table = "test_qwp_long_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "u UUID, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("u", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from LONG to UUID is not supported")
            );
        }
    }

    @Test
    public void testLongToVarchar() throws Exception {
        String table = "test_qwp_long_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .longColumn("v", 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .longColumn("v", Long.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testMultipleRowsAndBatching() throws Exception {
        String table = "test_qwp_multiple_rows";
        useTable(table);

        int rowCount = 1000;
        try (QwpWebSocketSender sender = createQwpSender()) {
            for (int i = 0; i < rowCount; i++) {
                sender.table(table)
                        .symbol("sym", "s" + (i % 10))
                        .longColumn("val", i)
                        .doubleColumn("dbl", i * 1.5)
                        .at((long) (i + 1) * 1_000_000, ChronoUnit.MICROS);
            }
            sender.flush();
        }

        assertTableSizeEventually(table, rowCount);
    }

    @Test
    public void testNullStringToBoolean() throws Exception {
        String table = "test_qwp_null_string_to_boolean";
        useTable(table);
        execute("CREATE TABLE " + table + " (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "true")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "b\tts\n" +
                        "true\t1970-01-01T00:00:01.000000000Z\n" +
                        "false\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToByte() throws Exception {
        String table = "test_qwp_null_string_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "42")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToChar() throws Exception {
        String table = "test_qwp_null_string_to_char";
        useTable(table);
        execute("CREATE TABLE " + table + " (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("c", "A")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("c", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "c\tts\n" +
                        "A\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT c, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToDate() throws Exception {
        String table = "test_qwp_null_string_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (d DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "2022-02-25T00:00:00.000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToDecimal() throws Exception {
        String table = "test_qwp_null_string_to_decimal";
        useTable(table);
        execute("CREATE TABLE " + table + " (d DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "123.45")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToFloat() throws Exception {
        String table = "test_qwp_null_string_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("f", "3.14")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("f", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "f\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToGeoHash() throws Exception {
        String table = "test_qwp_null_string_to_geohash";
        useTable(table);
        execute("CREATE TABLE " + table + " (g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("g", "s09wh")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("g", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "g\tts\n" +
                        "s09wh\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT g, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToLong256() throws Exception {
        String table = "test_qwp_null_string_to_long256";
        useTable(table);
        execute("CREATE TABLE " + table + " (l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("l", "0x01")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("l", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "l\tts\n" +
                        "0x01\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToNumeric() throws Exception {
        String table = "test_qwp_null_string_to_numeric";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "l LONG, " +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("i", "42")
                    .stringColumn("l", "100")
                    .stringColumn("d", "3.14")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("i", null)
                    .stringColumn("l", null)
                    .stringColumn("d", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "i\tl\td\tts\n" +
                        "42\t100\t3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\tnull\tnull\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT i, l, d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToShort() throws Exception {
        String table = "test_qwp_null_string_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("s", "42")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToSymbol() throws Exception {
        String table = "test_qwp_null_string_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("s", "alpha")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "alpha\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToTimestamp() throws Exception {
        String table = "test_qwp_null_string_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (t TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("t", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToTimestampNs() throws Exception {
        String table = "test_qwp_null_string_to_timestamp_ns";
        useTable(table);
        execute("CREATE TABLE " + table + " (t TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("t", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToUuid() throws Exception {
        String table = "test_qwp_null_string_to_uuid";
        useTable(table);
        execute("CREATE TABLE " + table + " (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("u", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "u\tts\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT u, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullStringToVarchar() throws Exception {
        String table = "test_qwp_null_string_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("v", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullSymbolToString() throws Exception {
        String table = "test_qwp_null_symbol_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("s", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("s", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullSymbolToSymbol() throws Exception {
        String table = "test_qwp_null_symbol_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("s", "alpha")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("s", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "alpha\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testNullSymbolToVarchar() throws Exception {
        String table = "test_qwp_null_symbol_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("v", null)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "null\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShort() throws Exception {
        String table = "test_qwp_short";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // Short.MIN_VALUE is the null sentinel for SHORT
            sender.table(table)
                    .shortColumn("s", Short.MIN_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", (short) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", Short.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
    }

    @Test
    public void testShortToBooleanCoercionError() throws Exception {
        String table = "test_qwp_short_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("b", (short) 1)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning SHORT and BOOLEAN but got: " + msg,
                    msg.contains("SHORT") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testShortToByte() throws Exception {
        String table = "test_qwp_short_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("b", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("b", (short) -128)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("b", (short) 127)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-128\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToByteOverflowError() throws Exception {
        String table = "test_qwp_short_to_byte_overflow";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("b", (short) 128)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected overflow error but got: " + msg,
                    msg.contains("integer value 128 out of range for BYTE")
            );
        }
    }

    @Test
    public void testShortToCharCoercionError() throws Exception {
        String table = "test_qwp_short_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "c CHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("c", (short) 65)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected error mentioning SHORT and CHAR but got: " + msg,
                    msg.contains("SHORT") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testShortToDate() throws Exception {
        String table = "test_qwp_short_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DATE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            // 1000 millis = 1 second
            sender.table(table)
                    .shortColumn("d", (short) 1000)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1970-01-01T00:00:01.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal128() throws Exception {
        String table = "test_qwp_short_to_decimal128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", Short.MAX_VALUE)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", Short.MIN_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "32767.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-32768.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal16() throws Exception {
        String table = "test_qwp_short_to_decimal16";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(4, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal256() throws Exception {
        String table = "test_qwp_short_to_decimal256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal32() throws Exception {
        String table = "test_qwp_short_to_decimal32";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(6, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal64() throws Exception {
        String table = "test_qwp_short_to_decimal64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.00\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.00\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDecimal8() throws Exception {
        String table = "test_qwp_short_to_decimal8";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 5)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -9)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "5.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToDouble() throws Exception {
        String table = "test_qwp_short_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("d", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("d", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToFloat() throws Exception {
        String table = "test_qwp_short_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("f", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("f", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "f\tts\n" +
                        "42.0\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100.0\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_short_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "g GEOHASH(4c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("g", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error mentioning SHORT but got: " + msg,
                    msg.contains("type coercion from SHORT to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testShortToInt() throws Exception {
        String table = "test_qwp_short_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("i", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("i", Short.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "i\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "32767\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToLong() throws Exception {
        String table = "test_qwp_short_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("l", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("l", Short.MAX_VALUE)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "l\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "32767\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToLong256CoercionError() throws Exception {
        String table = "test_qwp_short_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v LONG256, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("v", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from SHORT to LONG256 is not supported")
            );
        }
    }

    @Test
    public void testShortToString() throws Exception {
        String table = "test_qwp_short_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("s", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", (short) 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToSymbol() throws Exception {
        String table = "test_qwp_short_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("s", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", (short) -1)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("s", (short) 0)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToTimestamp() throws Exception {
        String table = "test_qwp_short_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "t TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("t", (short) 1000)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("t", (short) 0)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "t\tts\n" +
                        "1970-01-01T00:00:00.001000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testShortToUuidCoercionError() throws Exception {
        String table = "test_qwp_short_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "u UUID, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("u", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from SHORT to UUID is not supported")
            );
        }
    }

    @Test
    public void testShortToVarchar() throws Exception {
        String table = "test_qwp_short_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .shortColumn("v", (short) 42)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("v", (short) -100)
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .shortColumn("v", Short.MAX_VALUE)
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "v\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "32767\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testString() throws Exception {
        String table = "test_qwp_string";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("s", "hello world")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", "non-ascii äöü")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", "")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", null)
                    .at(4_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 4);
        assertSqlEventually(
                "s\ttimestamp\n" +
                        "hello world\t1970-01-01T00:00:01.000000000Z\n" +
                        "non-ascii äöü\t1970-01-01T00:00:02.000000000Z\n" +
                        "\t1970-01-01T00:00:03.000000000Z\n" +
                        "null\t1970-01-01T00:00:04.000000000Z\n",
                "SELECT s, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testStringToBoolean() throws Exception {
        String table = "test_qwp_string_to_boolean";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "true")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "false")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "1")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "0")
                    .at(4_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "TRUE")
                    .at(5_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 5);
        assertSqlEventually(
                "b\tts\n" +
                        "true\t1970-01-01T00:00:01.000000000Z\n" +
                        "false\t1970-01-01T00:00:02.000000000Z\n" +
                        "true\t1970-01-01T00:00:03.000000000Z\n" +
                        "false\t1970-01-01T00:00:04.000000000Z\n" +
                        "true\t1970-01-01T00:00:05.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToBooleanParseError() throws Exception {
        String table = "test_qwp_string_to_boolean_err";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BOOLEAN, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "yes")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse boolean from string")
            );
        }
    }

    @Test
    public void testStringToByte() throws Exception {
        String table = "test_qwp_string_to_byte";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "42")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "-128")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("b", "127")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "b\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-128\t1970-01-01T00:00:02.000000000Z\n" +
                        "127\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT b, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToByteParseError() throws Exception {
        String table = "test_qwp_string_to_byte_err";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "b BYTE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("b", "abc")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse BYTE from string")
            );
        }
    }

    @Test
    public void testStringToChar() throws Exception {
        String table = "test_qwp_string_to_char";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "c CHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("c", "A")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("c", "Hello")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "c\tts\n" +
                        "A\t1970-01-01T00:00:01.000000000Z\n" +
                        "H\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT c, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDate() throws Exception {
        String table = "test_qwp_string_to_date";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DATE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "2022-02-25T00:00:00.000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "d\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDateParseError() throws Exception {
        String table = "test_qwp_string_to_date_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_date")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse DATE from string") && msg.contains("not_a_date")
            );
        }
    }

    @Test
    public void testStringToDecimal128() throws Exception {
        String table = "test_qwp_string_to_dec128";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(38, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "123.45")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-99.99")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDecimal16() throws Exception {
        String table = "test_qwp_string_to_dec16";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(4, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "12.5")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-99.9")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "12.5\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.9\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDecimal256() throws Exception {
        String table = "test_qwp_string_to_dec256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(76, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "123.45")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-99.99")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDecimal32() throws Exception {
        String table = "test_qwp_string_to_dec32";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(6, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "1234.56")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-999.99")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1234.56\t1970-01-01T00:00:01.000000000Z\n" +
                        "-999.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDecimal64() throws Exception {
        String table = "test_qwp_string_to_dec64";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(18, 2), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "123.45")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-99.99")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "123.45\t1970-01-01T00:00:01.000000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDecimal8() throws Exception {
        String table = "test_qwp_string_to_dec8";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DECIMAL(2, 1), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "1.5")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-9.9")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "1.5\t1970-01-01T00:00:01.000000000Z\n" +
                        "-9.9\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDouble() throws Exception {
        String table = "test_qwp_string_to_double";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "d DOUBLE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("d", "3.14")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("d", "-2.718")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "d\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "-2.718\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT d, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToDoubleParseError() throws Exception {
        String table = "test_qwp_string_to_double_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_number")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse DOUBLE from string") && msg.contains("not_a_number")
            );
        }
    }

    @Test
    public void testStringToFloat() throws Exception {
        String table = "test_qwp_string_to_float";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "f FLOAT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("f", "3.14")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("f", "-2.5")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "f\tts\n" +
                        "3.14\t1970-01-01T00:00:01.000000000Z\n" +
                        "-2.5\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT f, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToFloatParseError() throws Exception {
        String table = "test_qwp_string_to_float_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_number")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse FLOAT from string") && msg.contains("not_a_number")
            );
        }
    }

    @Test
    public void testStringToGeoHash() throws Exception {
        String table = "test_qwp_string_to_geohash";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "g GEOHASH(5c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("g", "s24se")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("g", "u33dc")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "g\tts\n" +
                        "s24se\t1970-01-01T00:00:01.000000000Z\n" +
                        "u33dc\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT g, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToGeoHashParseError() throws Exception {
        String table = "test_qwp_string_to_geohash_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "!!!")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse geohash from string") && msg.contains("!!!")
            );
        }
    }

    @Test
    public void testStringToInt() throws Exception {
        String table = "test_qwp_string_to_int";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "i INT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("i", "42")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("i", "-100")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("i", "0")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "i\tts\n" +
                        "42\t1970-01-01T00:00:01.000000000Z\n" +
                        "-100\t1970-01-01T00:00:02.000000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT i, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToIntParseError() throws Exception {
        String table = "test_qwp_string_to_int_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_number")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse INT from string") && msg.contains("not_a_number")
            );
        }
    }

    @Test
    public void testStringToLong() throws Exception {
        String table = "test_qwp_string_to_long";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("l", "1000000000000")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("l", "-1")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "l\tts\n" +
                        "1000000000000\t1970-01-01T00:00:01.000000000Z\n" +
                        "-1\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToLong256() throws Exception {
        String table = "test_qwp_string_to_long256";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "l LONG256, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("l", "0x01")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "l\tts\n" +
                        "0x01\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT l, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToLong256ParseError() throws Exception {
        String table = "test_qwp_string_to_long256_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_long256")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse long256 from string") && msg.contains("not_a_long256")
            );
        }
    }

    @Test
    public void testStringToLongParseError() throws Exception {
        String table = "test_qwp_string_to_long_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_number")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse LONG from string") && msg.contains("not_a_number")
            );
        }
    }

    @Test
    public void testStringToShort() throws Exception {
        String table = "test_qwp_string_to_short";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("s", "1000")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", "-32768")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", "32767")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\tts\n" +
                        "1000\t1970-01-01T00:00:01.000000000Z\n" +
                        "-32768\t1970-01-01T00:00:02.000000000Z\n" +
                        "32767\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToShortParseError() throws Exception {
        String table = "test_qwp_string_to_short_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_number")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse SHORT from string") && msg.contains("not_a_number")
            );
        }
    }

    @Test
    public void testStringToSymbol() throws Exception {
        String table = "test_qwp_string_to_symbol";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SYMBOL, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("s", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("s", "world")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "world\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToTimestamp() throws Exception {
        String table = "test_qwp_string_to_timestamp";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "t TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "t\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT t, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToTimestampNs() throws Exception {
        String table = "test_qwp_string_to_timestamp_ns";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "ts_col TIMESTAMP_NS, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("ts_col", "2022-02-25T00:00:00.000000Z")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "ts_col\tts\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT ts_col, ts FROM " + table);
    }

    @Test
    public void testStringToTimestampParseError() throws Exception {
        String table = "test_qwp_string_to_timestamp_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not_a_timestamp")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse timestamp from string") && msg.contains("not_a_timestamp")
            );
        }
    }

    @Test
    public void testStringToUuid() throws Exception {
        String table = "test_qwp_string_to_uuid";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "u UUID, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "u\tts\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT u, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testStringToUuidParseError() throws Exception {
        String table = "test_qwp_string_to_uuid_parse_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "not-a-uuid")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected parse error but got: " + msg,
                    msg.contains("cannot parse UUID from string") && msg.contains("not-a-uuid")
            );
        }
    }

    @Test
    public void testStringToVarchar() throws Exception {
        String table = "test_qwp_string_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .stringColumn("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .stringColumn("v", "world")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "world\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testSymbol() throws Exception {
        String table = "test_qwp_symbol";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("s", "alpha")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("s", "beta")
                    .at(2_000_000, ChronoUnit.MICROS);
            // repeated value reuses dictionary entry
            sender.table(table)
                    .symbol("s", "alpha")
                    .at(3_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 3);
        assertSqlEventually(
                "s\ttimestamp\n" +
                        "alpha\t1970-01-01T00:00:01.000000000Z\n" +
                        "beta\t1970-01-01T00:00:02.000000000Z\n" +
                        "alpha\t1970-01-01T00:00:03.000000000Z\n",
                "SELECT s, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testSymbolToBooleanCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testSymbolToByteCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("BYTE")
            );
        }
    }

    @Test
    public void testSymbolToCharCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testSymbolToDateCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("DATE")
            );
        }
    }

    @Test
    public void testSymbolToDecimalCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_decimal_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("DECIMAL")
            );
        }
    }

    @Test
    public void testSymbolToDoubleCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("DOUBLE")
            );
        }
    }

    @Test
    public void testSymbolToFloatCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("FLOAT")
            );
        }
    }

    @Test
    public void testSymbolToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("GEOHASH")
            );
        }
    }

    @Test
    public void testSymbolToIntCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testSymbolToLong256CoercionError() throws Exception {
        String table = "test_qwp_symbol_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("LONG256")
            );
        }
    }

    @Test
    public void testSymbolToLongCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("LONG")
            );
        }
    }

    @Test
    public void testSymbolToShortCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("SHORT")
            );
        }
    }

    @Test
    public void testSymbolToString() throws Exception {
        String table = "test_qwp_symbol_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("s", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("s", "world")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "s\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "world\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testSymbolToTimestampCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_timestamp_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testSymbolToTimestampNsCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_timestamp_ns_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("TIMESTAMP")
            );
        }
    }

    @Test
    public void testSymbolToUuidCoercionError() throws Exception {
        String table = "test_qwp_symbol_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write SYMBOL") && msg.contains("UUID")
            );
        }
    }

    @Test
    public void testSymbolToVarchar() throws Exception {
        String table = "test_qwp_symbol_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("v", "hello")
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .symbol("v", "world")
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "v\tts\n" +
                        "hello\t1970-01-01T00:00:01.000000000Z\n" +
                        "world\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testTimestampMicros() throws Exception {
        String table = "test_qwp_timestamp_micros";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
            sender.table(table)
                    .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "ts_col\ttimestamp\n" +
                        "2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT ts_col, timestamp FROM " + table);
    }

    @Test
    public void testTimestampMicrosToNanos() throws Exception {
        String table = "test_qwp_timestamp_micros_to_nanos";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "ts_col TIMESTAMP_NS, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsMicros = 1_645_747_200_111_111L; // 2022-02-25T00:00:00Z
            sender.table(table)
                    .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        // Microseconds scaled to nanoseconds
        assertSqlEventually(
                "ts_col\tts\n" +
                        "2022-02-25T00:00:00.111111000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT ts_col, ts FROM " + table);
    }

    @Test
    public void testTimestampNanos() throws Exception {
        String table = "test_qwp_timestamp_nanos";
        useTable(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsNanos = 1_645_747_200_000_000_000L; // 2022-02-25T00:00:00Z in nanos
            sender.table(table)
                    .timestampColumn("ts_col", tsNanos, ChronoUnit.NANOS)
                    .at(tsNanos, ChronoUnit.NANOS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
    }

    @Test
    public void testTimestampNanosToMicros() throws Exception {
        String table = "test_qwp_timestamp_nanos_to_micros";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "ts_col TIMESTAMP, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsNanos = 1_645_747_200_123_456_789L;
            sender.table(table)
                    .timestampColumn("ts_col", tsNanos, ChronoUnit.NANOS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        // Nanoseconds truncated to microseconds
        assertSqlEventually(
                "ts_col\tts\n" +
                        "2022-02-25T00:00:00.123456000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT ts_col, ts FROM " + table);
    }

    @Test
    public void testTimestampToBooleanCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testTimestampToByteCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("BYTE")
            );
        }
    }

    @Test
    public void testTimestampToCharCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testTimestampToDateCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("DATE")
            );
        }
    }

    @Test
    public void testTimestampToDecimalCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_decimal_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("DECIMAL")
            );
        }
    }

    @Test
    public void testTimestampToDoubleCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("DOUBLE")
            );
        }
    }

    @Test
    public void testTimestampToFloatCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("FLOAT")
            );
        }
    }

    @Test
    public void testTimestampToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("GEOHASH")
            );
        }
    }

    @Test
    public void testTimestampToIntCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("INT")
            );
        }
    }

    @Test
    public void testTimestampToLong256CoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("LONG256")
            );
        }
    }

    @Test
    public void testTimestampToLongCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("LONG")
            );
        }
    }

    @Test
    public void testTimestampToShortCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("SHORT")
            );
        }
    }

    @Test
    public void testTimestampToString() throws Exception {
        String table = "test_qwp_timestamp_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
            sender.table(table)
                    .timestampColumn("s", tsMicros, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "s\tts\n" +
                        "2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT s, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testTimestampToSymbolCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testTimestampToUuidCoercionError() throws Exception {
        String table = "test_qwp_timestamp_to_uuid_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write TIMESTAMP") && msg.contains("UUID")
            );
        }
    }

    @Test
    public void testTimestampToVarchar() throws Exception {
        String table = "test_qwp_timestamp_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
            sender.table(table)
                    .timestampColumn("v", tsMicros, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "v\tts\n" +
                        "2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT v, ts FROM " + table + " ORDER BY ts");
    }

    @Test
    public void testUuid() throws Exception {
        String table = "test_qwp_uuid";
        useTable(table);

        UUID uuid1 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        UUID uuid2 = UUID.fromString("11111111-2222-3333-4444-555555555555");

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .uuidColumn("u", uuid1.getLeastSignificantBits(), uuid1.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.table(table)
                    .uuidColumn("u", uuid2.getLeastSignificantBits(), uuid2.getMostSignificantBits())
                    .at(2_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 2);
        assertSqlEventually(
                "u\ttimestamp\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000000Z\n" +
                        "11111111-2222-3333-4444-555555555555\t1970-01-01T00:00:02.000000000Z\n",
                "SELECT u, timestamp FROM " + table + " ORDER BY timestamp");
    }

    @Test
    public void testUuidToBooleanCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_boolean_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write UUID") && msg.contains("BOOLEAN")
            );
        }
    }

    @Test
    public void testUuidToByteCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_byte_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToCharCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_char_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write UUID") && msg.contains("CHAR")
            );
        }
    }

    @Test
    public void testUuidToDateCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_date_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToDoubleCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_double_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToFloatCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_float_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToGeoHashCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_geohash_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToIntCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_int_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToLong256CoercionError() throws Exception {
        String table = "test_qwp_uuid_to_long256_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToLongCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_long_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to") && msg.contains("is not supported")
            );
        }
    }

    @Test
    public void testUuidToShortCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_short_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s SHORT, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("s", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("type coercion from UUID to SHORT is not supported")
            );
        }
    }

    @Test
    public void testUuidToString() throws Exception {
        String table = "test_qwp_uuid_to_string";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "s STRING, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .uuidColumn("s", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "s\tts\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT s, ts FROM " + table);
    }

    @Test
    public void testUuidToSymbolCoercionError() throws Exception {
        String table = "test_qwp_uuid_to_symbol_error";
        useTable(table);
        execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);
        try (QwpWebSocketSender sender = createQwpSender()) {
            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue(
                    "Expected coercion error but got: " + msg,
                    msg.contains("cannot write UUID") && msg.contains("SYMBOL")
            );
        }
    }

    @Test
    public void testUuidToVarchar() throws Exception {
        String table = "test_qwp_uuid_to_varchar";
        useTable(table);
        execute("CREATE TABLE " + table + " (" +
                "v VARCHAR, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "v\tts\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000000Z\n",
                "SELECT v, ts FROM " + table);
    }

    @Test
    public void testWriteAllTypesInOneRow() throws Exception {
        String table = "test_qwp_all_types";
        useTable(table);

        UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        double[] arr1d = {1.0, 2.0, 3.0};
        long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z

        try (QwpWebSocketSender sender = createQwpSender()) {
            sender.table(table)
                    .symbol("sym", "test_symbol")
                    .boolColumn("bool_col", true)
                    .shortColumn("short_col", (short) 42)
                    .intColumn("int_col", 100_000)
                    .longColumn("long_col", 1_000_000_000L)
                    .floatColumn("float_col", 2.5f)
                    .doubleColumn("double_col", 3.14)
                    .stringColumn("string_col", "hello")
                    .charColumn("char_col", 'Z')
                    .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                    .uuidColumn("uuid_col", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                    .long256Column("long256_col", 1, 0, 0, 0)
                    .doubleArray("arr_col", arr1d)
                    .decimalColumn("decimal_col", "99.99")
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
    }

    private QwpWebSocketSender createQwpSender() {
        return QwpWebSocketSender.connect(getQuestDbHost(), getHttpPort());
    }
}
