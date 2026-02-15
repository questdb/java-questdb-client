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
import io.questdb.client.std.Decimal256;
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

    // === Exact Type Match Tests ===

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
                    .decimalColumn("d", Decimal256.fromLong(42_000, 3))
                    .at(4_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 4);
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
                    msg.contains("loses precision") && msg.contains("123.456") && msg.contains("scale=2")
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
                    msg.contains("loses precision") && msg.contains("scale=1")
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
                "ts_col TIMESTAMP WITH TIME ZONE, " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        assertTableExistsEventually(table);

        try (QwpWebSocketSender sender = createQwpSender()) {
            long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z
            sender.table(table)
                    .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                    .at(1_000_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
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

    // === Helper Methods ===

    private QwpWebSocketSender createQwpSender() {
        return QwpWebSocketSender.connect(getQuestDbHost(), getHttpPort());
    }
}
