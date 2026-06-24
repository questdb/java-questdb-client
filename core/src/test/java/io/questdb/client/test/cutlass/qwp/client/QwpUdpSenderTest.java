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
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpUdpSenderTest {

    @Test
    public void testAdaptiveHeadroomFlushesCommittedRowsBeforeNextRowStarts() throws Exception {
        assertMemoryLeak(() -> {
            String alpha = repeat('a', 256);
            String beta = repeat('b', 256);
            String gamma = repeat('c', 256);
            List<ScenarioRow> rows = Arrays.asList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 1)
                                    .stringColumn("s", alpha)
                                    .atNow(),
                            "x", 1L,
                            "s", alpha),
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 2)
                                    .stringColumn("s", beta)
                                    .atNow(),
                            "x", 2L,
                            "s", beta),
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 3)
                                    .stringColumn("s", gamma)
                                    .atNow(),
                            "x", 3L,
                            "s", gamma)
            );

            int oneRowPacket = fullPacketSize(rows.subList(0, 1));
            int twoRowPacket = fullPacketSize(rows.subList(0, 2));
            int maxDatagramSize = oneRowPacket + 16;
            Assert.assertTrue("expected overflow boundary between rows 1 and 2", maxDatagramSize < twoRowPacket);

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t")
                        .longColumn("x", 1)
                        .stringColumn("s", alpha)
                        .atNow();
                Assert.assertEquals(0, nf.sendCount);

                sender.longColumn("x", 2)
                        .stringColumn("s", beta)
                        .atNow();
                Assert.assertEquals("expected adaptive post-commit flush for row 2", 2, nf.sendCount);

                sender.longColumn("x", 3)
                        .stringColumn("s", gamma)
                        .atNow();
                Assert.assertEquals("expected row 3 to start on a fresh datagram", 3, nf.sendCount);

                sender.flush();
            }

            RunResult result = new RunResult(nf.packets, nf.lengths, nf.sendCount);
            Assert.assertEquals(3, result.sendCount);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(nf.packets));
        });
    }

    @Test
    public void testAdaptiveHeadroomStateIsPerTable() throws Exception {
        assertMemoryLeak(() -> {
            String alpha = repeat('a', 256);
            String beta = repeat('b', 256);
            List<ScenarioRow> bigRows = Arrays.asList(
                    row("big", sender -> sender.table("big")
                                    .longColumn("x", 1)
                                    .stringColumn("s", alpha)
                                    .atNow(),
                            "x", 1L,
                            "s", alpha),
                    row("big", sender -> sender.table("big")
                                    .longColumn("x", 2)
                                    .stringColumn("s", beta)
                                    .atNow(),
                            "x", 2L,
                            "s", beta)
            );
            List<ScenarioRow> smallRows = Arrays.asList(
                    row("small", sender -> sender.table("small")
                                    .longColumn("x", 10)
                                    .atNow(),
                            "x", 10L),
                    row("small", sender -> sender.table("small")
                                    .longColumn("x", 11)
                                    .atNow(),
                            "x", 11L)
            );

            int oneBigRowPacket = fullPacketSize(bigRows.subList(0, 1));
            int twoBigRowPacket = fullPacketSize(bigRows);
            int twoSmallRowPacket = fullPacketSize(smallRows);
            int maxDatagramSize = oneBigRowPacket + 16;
            Assert.assertTrue("expected overflow boundary between big rows", maxDatagramSize < twoBigRowPacket);
            Assert.assertTrue("expected small rows to fit together", twoSmallRowPacket <= maxDatagramSize);

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("big")
                        .longColumn("x", 1)
                        .stringColumn("s", alpha)
                        .atNow();
                sender.longColumn("x", 2)
                        .stringColumn("s", beta)
                        .atNow();
                Assert.assertEquals(2, nf.sendCount);

                sender.table("small")
                        .longColumn("x", 10)
                        .atNow();
                Assert.assertEquals("big-table headroom must not flush small-table row 1", 2, nf.sendCount);

                sender.longColumn("x", 11)
                        .atNow();
                Assert.assertEquals("small-table rows should share a datagram", 2, nf.sendCount);

                sender.flush();
            }

            RunResult result = new RunResult(nf.packets, nf.lengths, nf.sendCount);
            Assert.assertEquals(3, result.sendCount);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(Arrays.asList(
                    decodedRow("big", "x", 1L, "s", alpha),
                    decodedRow("big", "x", 2L, "s", beta),
                    decodedRow("small", "x", 10L),
                    decodedRow("small", "x", 11L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testArrayWrapperStagingSnapshotsMutationAndCancelRow() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024);
                 DoubleArray doubleArray = new DoubleArray(2)) {
                sender.table("arrays");
                long[] longValues = {1, 2};

                doubleArray.append(1.5).append(2.5);
                sender.longArray("la", longValues);
                sender.doubleArray("da", doubleArray);

                longValues[0] = 9;
                longValues[1] = 10;
                doubleArray.clear();
                doubleArray.append(9.5).append(10.5);
                sender.cancelRow();

                sender.longArray("la", longValues);
                sender.doubleArray("da", doubleArray);
                longValues[0] = 100;
                longValues[1] = 200;
                doubleArray.clear();
                doubleArray.append(100.5).append(200.5);
                sender.atNow();
                sender.flush();
            }

            assertRowsEqual(
                    Collections.singletonList(decodedRow(
                            "arrays",
                            "la", longArrayValue(shape(2), 9, 10),
                            "da", doubleArrayValue(shape(2), 9.5, 10.5)
                    )),
                    decodeRows(nf.packets)
            );
        });
    }

    @Test
    public void testAtMicrosOversizeFailureRollsBackWithoutLeakingTimestampState() throws Exception {
        assertMemoryLeak(() -> {
            String large = repeat('x', 5000);
            List<ScenarioRow> oversizedRow = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("a", 2)
                                    .stringColumn("s", large)
                                    .at(2_000_000L, ChronoUnit.MICROS),
                            "a", 2L,
                            "s", large,
                            "", 2_000_000L)
            );
            int maxDatagramSize = fullPacketSize(oversizedRow) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .at(1_000_000L, ChronoUnit.MICROS);

                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("a", 2)
                                .stringColumn("s", large)
                                .at(2_000_000L, ChronoUnit.MICROS)
                );
                Assert.assertEquals(1, nf.sendCount);

                sender.longColumn("a", 3).at(3_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L, "", 1_000_000L),
                    decodedRow("t", "a", 3L, "", 3_000_000L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testAtNanosOversizeFailureRollsBackWithoutLeakingTimestampState() throws Exception {
        assertMemoryLeak(() -> {
            String large = repeat('x', 5000);
            List<ScenarioRow> oversizedRow = Collections.singletonList(
                    row("tn", sender -> sender.table("tn")
                                    .longColumn("a", 2)
                                    .stringColumn("s", large)
                                    .at(2_000_000L, ChronoUnit.NANOS),
                            "a", 2L,
                            "s", large,
                            "", 2_000_000L)
            );
            int maxDatagramSize = fullPacketSize(oversizedRow) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("tn")
                        .longColumn("a", 1)
                        .at(1_000_000L, ChronoUnit.NANOS);

                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("a", 2)
                                .stringColumn("s", large)
                                .at(2_000_000L, ChronoUnit.NANOS)
                );
                Assert.assertEquals(1, nf.sendCount);

                sender.longColumn("a", 3).at(3_000_000L, ChronoUnit.NANOS);
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("tn", "a", 1L, "", 1_000_000L),
                    decodedRow("tn", "a", 3L, "", 3_000_000L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testAtNowAllowsTimestampOnlyRowWhenTableHasColumns() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                // First row establishes a column schema
                sender.table("t")
                        .longColumn("x", 1)
                        .atNow();

                // Second row with atNow() and no user columns is valid:
                // the pre-existing column "x" gets null-padded.
                sender.table("t").atNow();
                sender.flush();
            }

            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "x", 1L),
                    decodedRow("t", "x", Long.MIN_VALUE)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testAtNowOversizeFailureRollsBackWithoutExplicitCancel() throws Exception {
        assertMemoryLeak(() -> {
            String large = repeat('x', 5000);
            List<ScenarioRow> oversizedRow = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("a", 2)
                                    .stringColumn("s", large)
                                    .atNow(),
                            "a", 2L,
                            "s", large)
            );
            int maxDatagramSize = fullPacketSize(oversizedRow) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("a", 2)
                                .stringColumn("s", large)
                                .atNow()
                );
                Assert.assertEquals(1, nf.sendCount);

                sender.longColumn("a", 3).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testAtNowRejectsEmptyRowOnFirstRow() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                // Calling atNow() with no columns on a fresh table (no
                // pre-existing columns) should be rejected -- it would
                // produce a degenerate datagram with no column data.
                assertThrowsContains("no columns were provided", () ->
                        sender.table("t").atNow()
                );

                // Sender must remain usable after the rejected empty row
                sender.table("t")
                        .longColumn("x", 1)
                        .atNow();
                sender.flush();
            }

            assertRowsEqual(Collections.singletonList(
                    decodedRow("t", "x", 1L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testBoundedSenderArrayReplayPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            long[] longValues = new long[128];
            double[] doubleValues = new double[128];
            for (int i = 0; i < 128; i++) {
                longValues[i] = i * 3L;
                doubleValues[i] = i * 1.25;
            }

            long[][] longMatrix = new long[8][8];
            double[][] doubleMatrix = new double[8][8];
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 8; j++) {
                    longMatrix[i][j] = i * 100L + j;
                    doubleMatrix[i][j] = i * 10.0 + j + 0.5;
                }
            }

            List<ScenarioRow> rows = Arrays.asList(
                    row("arrays", sender -> sender.table("arrays")
                                    .symbol("sym", "alpha")
                                    .longColumn("x", 1)
                                    .longArray("la", longValues)
                                    .doubleArray("da", doubleValues)
                                    .atNow(),
                            "sym", "alpha",
                            "x", 1L,
                            "la", longArrayValue(shape(128), longValues),
                            "da", doubleArrayValue(shape(128), doubleValues)),
                    row("arrays", sender -> sender.table("arrays")
                                    .symbol("sym", "beta")
                                    .longColumn("x", 2)
                                    .longArray("la", longMatrix)
                                    .doubleArray("da", doubleMatrix)
                                    .atNow(),
                            "sym", "beta",
                            "x", 2L,
                            "la", longArrayValue(shape(8, 8), flatten(longMatrix)),
                            "da", doubleArrayValue(shape(8, 8), flatten(doubleMatrix))),
                    row("arrays", sender -> sender.table("arrays")
                                    .symbol("sym", "gamma")
                                    .longColumn("x", 3)
                                    .longArray("la", (long[]) null)
                                    .doubleArray("da", (double[]) null)
                                    .atNow(),
                            "sym", "gamma",
                            "x", 3L,
                            "la", null,
                            "da", null)
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderHigherDimensionalDoubleArrayWrapperPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("arrays", sender -> {
                                try (DoubleArray doubleArray = new DoubleArray(2, 1, 1, 2)) {
                                    doubleArray.append(1.25).append(2.25).append(3.25).append(4.25);
                                    sender.table("arrays")
                                            .symbol("sym", "alpha")
                                            .longArray("la", new long[][][]{{{1, 2}}, {{3, 4}}})
                                            .doubleArray("da", doubleArray)
                                            .atNow();
                                }
                            },
                            "sym", "alpha",
                            "la", longArrayValue(shape(2, 1, 2), 1, 2, 3, 4),
                            "da", doubleArrayValue(shape(2, 1, 1, 2), 1.25, 2.25, 3.25, 4.25)),
                    row("arrays", sender -> {
                                try (DoubleArray doubleArray = new DoubleArray(1, 2, 1, 2)) {
                                    doubleArray.append(10.5).append(20.5).append(30.5).append(40.5);
                                    sender.table("arrays")
                                            .symbol("sym", "beta")
                                            .longArray("la", new long[][]{{10, 20}, {30, 40}})
                                            .doubleArray("da", doubleArray)
                                            .atNow();
                                }
                            },
                            "sym", "beta",
                            "la", longArrayValue(shape(2, 2), 10, 20, 30, 40),
                            "da", doubleArrayValue(shape(1, 2, 1, 2), 10.5, 20.5, 30.5, 40.5))
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderMixedNullablePaddingPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            String alpha = repeat('a', 256);
            String omega = repeat('z', 256);
            List<ScenarioRow> rows = Arrays.asList(
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v1")
                                    .longColumn("x", 1)
                                    .stringColumn("s", alpha)
                                    .atNow(),
                            "sym", "v1",
                            "x", 1L,
                            "s", alpha),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", null)
                                    .longColumn("x", 2)
                                    .atNow(),
                            "sym", null,
                            "x", 2L,
                            "s", null),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", null)
                                    .longColumn("x", 3)
                                    .stringColumn("s", null)
                                    .atNow(),
                            "sym", null,
                            "x", 3L,
                            "s", null),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v2")
                                    .longColumn("x", 4)
                                    .stringColumn("s", omega)
                                    .atNow(),
                            "sym", "v2",
                            "x", 4L,
                            "s", omega)
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderMixedTypesPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            String msg1 = repeat('m', 240);
            String msg2 = repeat('n', 240);
            List<ScenarioRow> rows = Arrays.asList(
                    row("mix", sender -> sender.table("mix")
                                    .symbol("sym", "alpha")
                                    .boolColumn("active", true)
                                    .longColumn("count", 1)
                                    .doubleColumn("value", 1.5)
                                    .stringColumn("msg", msg1)
                                    .decimalColumn("d64", Decimal64.fromLong(12345L, 2))
                                    .decimalColumn("d128", Decimal128.fromLong(678901234L, 4))
                                    .decimalColumn("d256", Decimal256.fromLong(9876543210L, 3))
                                    .timestampColumn("eventMicros", 123456L, ChronoUnit.MICROS)
                                    .timestampColumn("eventNanos", 999L, ChronoUnit.NANOS)
                                    .at(1_000_000L, ChronoUnit.MICROS),
                            "sym", "alpha",
                            "active", true,
                            "count", 1L,
                            "value", 1.5,
                            "msg", msg1,
                            "d64", decimal(12345L, 2),
                            "d128", decimal(678901234L, 4),
                            "d256", decimal(9876543210L, 3),
                            "eventMicros", 123456L,
                            "eventNanos", 999L,
                            "", 1_000_000L),
                    row("mix", sender -> sender.table("mix")
                                    .symbol("sym", "beta")
                                    .boolColumn("active", false)
                                    .longColumn("count", 2)
                                    .doubleColumn("value", 2.5)
                                    .stringColumn("msg", msg2)
                                    .decimalColumn("d64", Decimal64.fromLong(-67890L, 2))
                                    .decimalColumn("d128", Decimal128.fromLong(2222333344L, 4))
                                    .decimalColumn("d256", Decimal256.fromLong(7777777770L, 3))
                                    .timestampColumn("eventMicros", 654321L, ChronoUnit.MICROS)
                                    .timestampColumn("eventNanos", 12345L, ChronoUnit.NANOS)
                                    .at(2_000_000L, ChronoUnit.MICROS),
                            "sym", "beta",
                            "active", false,
                            "count", 2L,
                            "value", 2.5,
                            "msg", msg2,
                            "d64", decimal(-67890L, 2),
                            "d128", decimal(2222333344L, 4),
                            "d256", decimal(7777777770L, 3),
                            "eventMicros", 654321L,
                            "eventNanos", 12345L,
                            "", 2_000_000L)
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderNullableStringNullAcrossOverflowBoundaryPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            String alpha = repeat('a', 512);
            String marker1 = repeat('m', 64);
            String marker2 = repeat('n', 64);
            String marker3 = repeat('o', 64);
            String omega = repeat('z', 128);
            List<ScenarioRow> rows = Arrays.asList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 1)
                                    .stringColumn("s", alpha)
                                    .stringColumn("m", marker1)
                                    .atNow(),
                            "x", 1L,
                            "s", alpha,
                            "m", marker1),
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 2)
                                    .stringColumn("s", null)
                                    .stringColumn("m", marker2)
                                    .atNow(),
                            "x", 2L,
                            "s", null,
                            "m", marker2),
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 3)
                                    .stringColumn("s", omega)
                                    .stringColumn("m", marker3)
                                    .atNow(),
                            "x", 3L,
                            "s", omega,
                            "m", marker3)
            );
            int firstRowPacket = fullPacketSize(rows.subList(0, 1));
            int twoRowPacket = fullPacketSize(rows.subList(0, 2));
            int maxDatagramSize = firstRowPacket + 16;
            Assert.assertTrue("expected overflow boundary between row 1 and row 2", maxDatagramSize < twoRowPacket);

            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderOmittedNonNullableColumnsPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            String alpha = repeat('a', 256);
            String beta = repeat('b', 192);
            String omega = repeat('z', 256);
            List<ScenarioRow> rows = Arrays.asList(
                    row("mix", sender -> sender.table("mix")
                                    .longColumn("x", 1)
                                    .stringColumn("msg", alpha)
                                    .atNow(),
                            "x", 1L,
                            "msg", alpha),
                    row("mix", sender -> sender.table("mix")
                                    .stringColumn("msg", beta)
                                    .atNow(),
                            "x", Long.MIN_VALUE,
                            "msg", beta),
                    row("mix", sender -> sender.table("mix")
                                    .longColumn("x", 3)
                                    .stringColumn("msg", omega)
                                    .atNow(),
                            "x", 3L,
                            "msg", omega)
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderOutOfOrderExistingColumnsPreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("order", sender -> sender.table("order")
                                    .longColumn("a", 1)
                                    .stringColumn("b", "x")
                                    .symbol("c", "alpha")
                                    .atNow(),
                            "a", 1L,
                            "b", "x",
                            "c", "alpha"),
                    row("order", sender -> sender.table("order")
                                    .symbol("c", "beta")
                                    .stringColumn("b", "y")
                                    .longColumn("a", 2)
                                    .atNow(),
                            "a", 2L,
                            "b", "y",
                            "c", "beta"),
                    row("order", sender -> sender.table("order")
                                    .stringColumn("b", "z")
                                    .longColumn("a", 3)
                                    .symbol("c", null)
                                    .atNow(),
                            "a", 3L,
                            "b", "z",
                            "c", null)
            );

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderRepeatedOverflowBoundariesWithDistinctSymbolsPreserveRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            String payload = repeat('p', 256);
            List<ScenarioRow> rows = Arrays.asList(
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v0")
                                    .longColumn("x", 0)
                                    .stringColumn("s", payload)
                                    .atNow(),
                            "sym", "v0",
                            "x", 0L,
                            "s", payload),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v1")
                                    .longColumn("x", 1)
                                    .stringColumn("s", payload)
                                    .atNow(),
                            "sym", "v1",
                            "x", 1L,
                            "s", payload),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v2")
                                    .longColumn("x", 2)
                                    .stringColumn("s", payload)
                                    .atNow(),
                            "sym", "v2",
                            "x", 2L,
                            "s", payload),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v3")
                                    .longColumn("x", 3)
                                    .stringColumn("s", payload)
                                    .atNow(),
                            "sym", "v3",
                            "x", 3L,
                            "s", payload),
                    row("t", sender -> sender.table("t")
                                    .symbol("sym", "v4")
                                    .longColumn("x", 4)
                                    .stringColumn("s", payload)
                                    .atNow(),
                            "sym", "v4",
                            "x", 4L,
                            "s", payload)
            );
            int maxDatagramSize = fullPacketSize(rows.subList(0, 2)) - 1;

            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertEquals(rows.size(), result.sendCount);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBoundedSenderSchemaFlushThenOmittedNullableColumnsPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("schema", sender -> sender.table("schema")
                                    .longColumn("x", 1)
                                    .stringColumn("s", "alpha")
                                    .atNow(),
                            "x", 1L,
                            "s", "alpha"),
                    row("schema", sender -> sender.table("schema")
                                    .symbol("sym", "new")
                                    .longColumn("x", 2)
                                    .stringColumn("s", "beta")
                                    .atNow(),
                            "sym", "new",
                            "x", 2L,
                            "s", "beta"),
                    row("schema", sender -> sender.table("schema")
                                    .longColumn("x", 3)
                                    .atNow(),
                            "sym", null,
                            "x", 3L,
                            "s", null)
            );

            RunResult result = runScenario(rows, 1024 * 1024);

            Assert.assertEquals(2, result.sendCount);
            assertPacketsWithinLimit(result, 1024 * 1024);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testBufferViewThrowsForUdpSender() throws Exception {
        // The Sender public API exposes bufferView() but UDP has no
        // accumulated request buffer to peek at -- the contract is to
        // throw a LineSenderException naming the method.
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                try {
                    sender.bufferView();
                    Assert.fail("expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(
                            "message should name bufferView, was: " + e.getMessage(),
                            e.getMessage().contains("bufferView()"));
                }
            }
        });
    }

    @Test
    public void testCancelRowAfterMidRowSchemaChangeDoesNotLeakSchema() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.cancelRow();
                sender.longColumn("a", 4).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 4L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testCloseDropsInProgressRowButFlushesCommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t").longColumn("x", 1).atNow();
                sender.longColumn("x", 2);
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(decodedRow("t", "x", 1L)), decodeRows(nf.packets));
        });
    }

    @Test
    public void testDuplicateColumnAfterSchemaFlushReplayIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                assertThrowsContains("column 'a' already set for current row", () -> sender.longColumn("a", 4));

                sender.cancelRow();
                sender.longColumn("a", 5).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 5L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testEstimateMatchesActualEncodedSize() throws Exception {
        assertMemoryLeak(() -> {
            auditEstimateWithStableSchemaAndNullableValues();
            auditEstimateAcrossSymbolDictionaryVarintBoundary();
        });
    }

    @Test
    public void testFirstRowAllowsMultipleNewColumnsAndEncodesRow() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("a", 1)
                                    .doubleColumn("b", 2.0)
                                    .stringColumn("c", "x")
                                    .atNow(),
                            "a", 1L,
                            "b", 2.0,
                            "c", "x")
            );

            RunResult result = runScenario(rows, 1024 * 1024);
            Assert.assertEquals(1, result.sendCount);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testFlushPreservesPendingFillStateForCurrentTable() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .longColumn("b", 2)
                        .atNow();

                sender.flush();

                sender.longColumn("a", 3).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L, "b", 2L),
                    decodedRow("t", "a", 3L, "b", Long.MIN_VALUE)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testFlushWhileRowInProgressThrowsAndPreservesRow() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t").longColumn("x", 1);

                assertThrowsContains("Cannot flush buffer while row is in progress", sender::flush);

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(decodedRow("t", "x", 1L)), decodeRows(nf.packets));
        });
    }

    @Test
    public void testIrregularArrayRejectedDuringStagingAndSenderRemainsUsable() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("arrays");
                assertThrowsContains("irregular array shape", () ->
                        sender.doubleArray("da", new double[][]{{1.0}, {2.0, 3.0}})
                );

                sender.table("ok");
                sender.longColumn("x", 1).atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(decodedRow("ok", "x", 1L)), decodeRows(nf.packets));
        });
    }

    @Test
    public void testIrregularArrayRejectedDuringStagingDoesNotLeakColumnIntoSameTable() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("arrays");
                assertThrowsContains("irregular array shape", () ->
                        sender.doubleArray("bad", new double[][]{{1.0}, {2.0, 3.0}})
                );

                sender.longColumn("x", 1).atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(decodedRow("arrays", "x", 1L)), decodeRows(nf.packets));
        });
    }

    @Test
    public void testLongArrayWrapperStagingSnapshotsMutation() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024);
                 LongArray longArray = new LongArray(2, 2)) {
                sender.table("arrays");

                longArray.append(1).append(2).append(3).append(4);
                sender.longArray("la", longArray);

                longArray.clear();
                longArray.append(10).append(20).append(30).append(40);
                sender.atNow();
                sender.flush();
            }

            assertRowsEqual(
                    Collections.singletonList(decodedRow(
                            "arrays",
                            "la", longArrayValue(shape(2, 2), 1, 2, 3, 4)
                    )),
                    decodeRows(nf.packets)
            );
        });
    }

    @Test
    public void testMixingAtNowAndAtMicrosAfterCommittedRowsSplitsDatagramAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("x", 1)
                        .atNow();

                sender.longColumn("x", 2);
                sender.at(2, ChronoUnit.MICROS);
                Assert.assertEquals(1, nf.sendCount);

                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "x", 1L),
                    decodedRow("t", "x", 2L, "", 2L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testNullableArrayReplayKeepsNullArrayStateWithoutReflection() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longArray("la", new long[]{1, 2})
                        .doubleArray("da", new double[]{1.0, 2.0})
                        .atNow();

                sender.stageNullLongArrayForTest("la");
                sender.stageNullDoubleArrayForTest("da");
                sender.longColumn("b", 3);

                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();

                QwpTableBuffer tableBuffer = sender.currentTableBufferForTest();
                Assert.assertNotNull(tableBuffer);
                Assert.assertEquals(1, tableBuffer.getRowCount());

                assertNullableArrayNullState(tableBuffer.getExistingColumn("la", TYPE_LONG_ARRAY));
                assertNullableArrayNullState(tableBuffer.getExistingColumn("da", TYPE_DOUBLE_ARRAY));

                QwpTableBuffer.ColumnBuffer longColumn = tableBuffer.getExistingColumn("b", TYPE_LONG);
                Assert.assertNotNull(longColumn);
                Assert.assertEquals(1, longColumn.getSize());
                Assert.assertEquals(1, longColumn.getValueCount());
                Assert.assertEquals(3L, Unsafe.getUnsafe().getLong(longColumn.getDataAddress()));
            }
        });
    }

    @Test
    public void testNullableStringPrefixFlushKeepsNullStateWithoutReflection() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("x", 1)
                        .stringColumn("s", "alpha")
                        .atNow();

                sender.longColumn("x", 2);
                sender.stringColumn("s", null);
                sender.longColumn("b", 3);

                Assert.assertEquals(1, nf.sendCount);

                QwpTableBuffer tableBuffer = sender.currentTableBufferForTest();
                Assert.assertNotNull(tableBuffer);
                Assert.assertEquals(0, tableBuffer.getRowCount());

                QwpTableBuffer.ColumnBuffer stringColumn = tableBuffer.getExistingColumn("s", TYPE_VARCHAR);
                assertNullableStringNullState(stringColumn);

                QwpTableBuffer.ColumnBuffer longColumn = tableBuffer.getExistingColumn("x", TYPE_LONG);
                Assert.assertNotNull(longColumn);
                Assert.assertEquals(1, longColumn.getSize());
                Assert.assertEquals(1, longColumn.getValueCount());
                Assert.assertEquals(2L, Unsafe.getUnsafe().getLong(longColumn.getDataAddress()));

                QwpTableBuffer.ColumnBuffer newColumn = tableBuffer.getExistingColumn("b", TYPE_LONG);
                Assert.assertNotNull(newColumn);
                Assert.assertEquals(1, newColumn.getSize());
                Assert.assertEquals(1, newColumn.getValueCount());
                Assert.assertEquals(3L, Unsafe.getUnsafe().getLong(newColumn.getDataAddress()));

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "x", 1L, "s", "alpha"),
                    decodedRow("t", "x", 2L, "s", null, "b", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testOversizedArrayRowRejectedUsesActualEncodedSize() throws Exception {
        assertMemoryLeak(() -> {
            long[] longValues = new long[1024];
            double[] doubleValues = new double[1024];
            for (int i = 0; i < 1024; i++) {
                longValues[i] = i;
                doubleValues[i] = i + 0.25;
            }

            List<ScenarioRow> rows = Collections.singletonList(
                    row("arrays", sender -> sender.table("arrays")
                                    .longColumn("x", 1)
                                    .longArray("la", longValues)
                                    .doubleArray("da", doubleValues)
                                    .atNow(),
                            "x", 1L,
                            "la", longArrayValue(shape(1024), longValues),
                            "da", doubleArrayValue(shape(1024), doubleValues))
            );
            int maxDatagramSize = fullPacketSize(rows) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("arrays");
                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("x", 1)
                                .longArray("la", longValues)
                                .doubleArray("da", doubleValues)
                                .atNow()
                );
            }

            Assert.assertEquals(0, nf.sendCount);
        });
    }

    @Test
    public void testOversizedRowAfterMidRowSchemaChangeCancelDoesNotLeakSchema() throws Exception {
        assertMemoryLeak(() -> {
            String large = repeat('x', 5000);
            List<ScenarioRow> oversizedRow = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("a", 2)
                                    .stringColumn("s", large)
                                    .atNow(),
                            "a", 2L,
                            "s", large)
            );
            int maxDatagramSize = fullPacketSize(oversizedRow) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                assertThrowsContains("single row exceeds maximum datagram size", () -> {
                    sender.stringColumn("s", large);
                    sender.atNow();
                });
                Assert.assertEquals(1, nf.sendCount);

                sender.cancelRow();
                sender.longColumn("a", 3).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testOversizedSingleRowRejectedAfterReplayUsesActualEncodedSize() throws Exception {
        assertMemoryLeak(() -> {
            String small = repeat('s', 32);
            String large = repeat('x', 5000);
            List<ScenarioRow> largeRow = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 2)
                                    .stringColumn("s", large)
                                    .atNow(),
                            "x", 2L,
                            "s", large)
            );
            int maxDatagramSize = fullPacketSize(largeRow) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t")
                        .longColumn("x", 1)
                        .stringColumn("s", small)
                        .atNow();

                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("x", 2)
                                .stringColumn("s", large)
                                .atNow()
                );
            }

            Assert.assertEquals(1, nf.sendCount);
            assertPacketsWithinLimit(new RunResult(nf.packets, nf.lengths, nf.sendCount), maxDatagramSize);
            assertRowsEqual(
                    Collections.singletonList(decodedRow("t", "x", 1L, "s", small)),
                    decodeRows(nf.packets)
            );
        });
    }

    @Test
    public void testOversizedSingleRowRejectedBeforeReplayUsesActualEncodedSize() throws Exception {
        assertMemoryLeak(() -> {
            String large = repeat('x', 5000);
            List<ScenarioRow> rows = Collections.singletonList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 1)
                                    .stringColumn("s", large)
                                    .atNow(),
                            "x", 1L,
                            "s", large)
            );
            int maxDatagramSize = fullPacketSize(rows) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("t");
                assertThrowsContains("single row exceeds maximum datagram size", () ->
                        sender.longColumn("x", 1)
                                .stringColumn("s", large)
                                .atNow()
                );
            }

            Assert.assertEquals(0, nf.sendCount);
        });
    }

    @Test
    public void testRepeatedUtf8SymbolsAcrossRowsPreserveRows() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("sym", sender -> sender.table("sym")
                                    .longColumn("x", 1)
                                    .symbol("sym", "東京")
                                    .atNow(),
                            "x", 1L,
                            "sym", "東京"),
                    row("sym", sender -> sender.table("sym")
                                    .longColumn("x", 2)
                                    .symbol("sym", "東京")
                                    .atNow(),
                            "x", 2L,
                            "sym", "東京"),
                    row("sym", sender -> sender.table("sym")
                                    .longColumn("x", 3)
                                    .symbol("sym", "Αθηνα")
                                    .atNow(),
                            "x", 3L,
                            "sym", "Αθηνα")
            );

            RunResult result = runScenario(rows, 1024 * 1024);
            Assert.assertEquals(1, result.sendCount);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testResetClearsBufferedRowsAndAllowsReuse() throws Exception {
        // Sender.reset() is the public escape hatch from a partially-built
        // batch. After it, the sender must accept a fresh table/row sequence
        // and a subsequent flush must emit only the post-reset row -- the
        // pre-reset accumulation must not leak into the wire.
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("scratch").longColumn("v", 1L).atNow();
                sender.table("scratch").longColumn("v", 2L).atNow();

                sender.reset();
                Assert.assertEquals("reset() must not flush", 0, nf.sendCount);

                sender.table("kept").longColumn("v", 99L).atNow();
                sender.flush();
                Assert.assertEquals(1, nf.sendCount);

                List<DecodedRow> emitted = decodeRows(nf.packets);
                Assert.assertEquals(1, emitted.size());
                Assert.assertEquals("kept", emitted.get(0).table);
                Assert.assertEquals(99L, emitted.get(0).values.get("v"));
            }
        });
    }

    @Test
    public void testSchemaChangeAfterOutOfOrderExistingColumnsPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("schema", sender -> sender.table("schema")
                                    .longColumn("a", 1)
                                    .stringColumn("b", "x")
                                    .atNow(),
                            "a", 1L,
                            "b", "x"),
                    row("schema", sender -> sender.table("schema")
                                    .symbol("c", "new")
                                    .stringColumn("b", "y")
                                    .longColumn("a", 2)
                                    .atNow(),
                            "a", 2L,
                            "b", "y",
                            "c", "new"),
                    row("schema", sender -> sender.table("schema")
                                    .symbol("c", "next")
                                    .longColumn("a", 3)
                                    .stringColumn("b", "z")
                                    .atNow(),
                            "a", 3L,
                            "b", "z",
                            "c", "next")
            );

            RunResult result = runScenario(rows, 1024 * 1024);

            Assert.assertEquals(2, result.sendCount);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testSchemaChangeMidRowAllowsMultipleNewColumnsAfterReplay() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.stringColumn("c", "x");
                sender.longColumn("d", 4);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 2L, "b", 3L, "c", "x", "d", 4L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSchemaChangeMidRowAllowsMultipleNewColumnsAfterReplayWithoutDatagramTracking() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.stringColumn("c", "x");
                sender.longColumn("d", 4);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 2L, "b", 3L, "c", "x", "d", 4L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSchemaChangeMidRowFlushesImmediatelyAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();

                sender.longColumn("a", 2);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                Assert.assertEquals(1, nf.sendCount);
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L),
                    decodedRow("t", "a", 2L, "b", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSchemaChangeMidRowPreservesExistingArrayValues() throws Exception {
        assertMemoryLeak(() -> {
            double[] first = {1.0, 2.0};
            double[] second = {3.5, 4.5, 5.5};

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .doubleArray("da", first)
                        .atNow();

                sender.longColumn("a", 2);
                sender.doubleArray("da", second);
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L, "da", doubleArrayValue(shape(2), first)),
                    decodedRow("t", "a", 2L, "da", doubleArrayValue(shape(3), second), "b", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSchemaChangeMidRowPreservesExistingStringAndSymbolValues() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .stringColumn("s", "alpha")
                        .symbol("sym", "one")
                        .atNow();

                sender.longColumn("a", 2);
                sender.stringColumn("s", "beta");
                sender.symbol("sym", "two");
                sender.longColumn("b", 3);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "a", 1L, "s", "alpha", "sym", "one"),
                    decodedRow("t", "a", 2L, "s", "beta", "sym", "two", "b", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSchemaChangeWithCommittedRowsFlushesImmediately() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .longColumn("a", 1)
                        .atNow();
                Assert.assertEquals(0, nf.sendCount);

                sender.longColumn("b", 2);
                Assert.assertEquals(1, nf.sendCount);

                sender.atNow();
                Assert.assertEquals(1, nf.sendCount);

                sender.flush();
                Assert.assertEquals(2, nf.sendCount);
            }
        });
    }

    @Test
    public void testSimpleLongRowUsesScatterSendPath() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t").longColumn("x", 42L).atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            Assert.assertEquals(1, nf.scatterSendCount);
            Assert.assertEquals(0, nf.rawSendCount);
            Assert.assertTrue("expected multiple segments for header/schema/data", nf.segmentCounts.get(0) > 1);
            assertRowsEqual(Collections.singletonList(decodedRow("t", "x", 42L)), decodeRows(nf.packets));
        });
    }

    @Test
    public void testSwitchTableWhileRowInProgressThrowsAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                sender.table("t1").longColumn("x", 1);

                assertThrowsContains("Cannot switch tables while row is in progress",
                        () -> sender.table("t2"));

                sender.atNow();
                sender.table("t2").longColumn("y", 2).atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqualIgnoringOrder(
                    Arrays.asList(decodedRow("t1", "x", 1L), decodedRow("t2", "y", 2L)),
                    decodeRows(nf.packets)
            );
        });
    }

    @Test
    public void testSymbolBoundary127To128PreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = new ArrayList<>(130);
            for (int i = 0; i < 130; i++) {
                final int value = i;
                rows.add(row("t", sender -> sender.table("t")
                                .symbol("sym", "v" + value)
                                .longColumn("x", value)
                                .atNow(),
                        "sym", "v" + value,
                        "x", (long) value));
            }

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testSymbolBoundary16383To16384PreservesRowsAndPacketLimit() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = new ArrayList<>(16_390);
            for (int i = 0; i < 16_390; i++) {
                final int value = i;
                rows.add(row("t", sender -> sender.table("t")
                                .symbol("sym", "v" + value)
                                .longColumn("x", value)
                                .atNow(),
                        "sym", "v" + value,
                        "x", (long) value));
            }

            int maxDatagramSize = fullPacketSize(rows) - 1;
            RunResult result = runScenario(rows, maxDatagramSize);

            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testSymbolPrefixFlushKeepsSingleRetainedDictionaryEntry() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
                sender.table("t")
                        .symbol("sym", "alpha")
                        .longColumn("x", 1)
                        .atNow();

                sender.symbol("sym", "beta");
                sender.longColumn("x", 2);
                sender.longColumn("b", 3);

                Assert.assertEquals(1, nf.sendCount);

                QwpTableBuffer tableBuffer = sender.currentTableBufferForTest();
                Assert.assertNotNull(tableBuffer);
                Assert.assertEquals(0, tableBuffer.getRowCount());

                QwpTableBuffer.ColumnBuffer symbolColumn = tableBuffer.getExistingColumn("sym", TYPE_SYMBOL);
                Assert.assertNotNull(symbolColumn);
                Assert.assertEquals(1, symbolColumn.getSize());
                Assert.assertEquals(1, symbolColumn.getValueCount());
                Assert.assertEquals(1, symbolColumn.getSymbolDictionarySize());
                Assert.assertEquals("beta", symbolColumn.getSymbolValue(0));
                Assert.assertTrue(symbolColumn.hasSymbol("beta"));
                Assert.assertFalse(symbolColumn.hasSymbol("alpha"));
                Assert.assertEquals(0, Unsafe.getUnsafe().getInt(symbolColumn.getDataAddress()));

                sender.atNow();
                sender.flush();
            }

            Assert.assertEquals(2, nf.sendCount);
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "sym", "alpha", "x", 1L),
                    decodedRow("t", "sym", "beta", "x", 2L, "b", 3L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testTableRejectsEmptyName() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                assertThrowsContains("table name cannot be empty", () ->
                        sender.table("")
                );

                // Sender must remain usable after the rejected table name
                sender.table("valid")
                        .longColumn("x", 1)
                        .atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(
                    decodedRow("valid", "x", 1L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testTableRejectsInvalidCharacters() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                assertThrowsContains("table name contains illegal characters", () ->
                        sender.table("has?question")
                );
                assertThrowsContains("table name contains illegal characters", () ->
                        sender.table("has/slash")
                );
                assertThrowsContains("table name contains illegal characters", () ->
                        sender.table(".leading_dot")
                );

                // Sender must remain usable after rejected names
                sender.table("valid")
                        .longColumn("x", 1)
                        .atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
        });
    }

    @Test
    public void testTableRejectsNullName() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                assertThrowsContains("table name cannot be empty", () ->
                        sender.table(null)
                );

                // Sender must remain usable after the rejected null table name
                sender.table("valid")
                        .longColumn("x", 1)
                        .atNow();
                sender.flush();
            }

            Assert.assertEquals(1, nf.sendCount);
            assertRowsEqual(Collections.singletonList(
                    decodedRow("valid", "x", 1L)
            ), decodeRows(nf.packets));
        });
    }

    @Test
    public void testTimestampOnlyRows() throws Exception {
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                // at() with no other columns: designated timestamp is staged
                sender.table("t").at(1_000L, ChronoUnit.MICROS);
                // atNow() with no other columns: server assigns the timestamp
                sender.table("t").atNow();
                sender.flush();
            }

            List<DecodedRow> rows = decodeRows(nf.packets);
            Assert.assertEquals("expected 2 timestamp-only rows", 2, rows.size());
            assertRowsEqual(Arrays.asList(
                    decodedRow("t", "", 1_000L),
                    decodedRow("t", "", null)
            ), rows);
        });
    }

    @Test
    public void testUnboundedSenderOmittedNullableAndNonNullableColumnsPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 1)
                                    .stringColumn("s", "alpha")
                                    .symbol("sym", "one")
                                    .atNow(),
                            "x", 1L,
                            "s", "alpha",
                            "sym", "one"),
                    row("t", sender -> sender.table("t")
                                    .stringColumn("s", "beta")
                                    .atNow(),
                            "x", Long.MIN_VALUE,
                            "s", "beta",
                            "sym", null),
                    row("t", sender -> sender.table("t")
                                    .longColumn("x", 3)
                                    .atNow(),
                            "x", 3L,
                            "s", null,
                            "sym", null)
            );

            RunResult result = runScenario(rows, 0);

            Assert.assertEquals(1, result.sendCount);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testUnboundedSenderWideSchemaWithLowIndexWritePreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            List<ScenarioRow> rows = Arrays.asList(
                    row("wide", sender -> sender.table("wide")
                                    .longColumn("c0", 0)
                                    .longColumn("c1", 1)
                                    .longColumn("c2", 2)
                                    .longColumn("c3", 3)
                                    .longColumn("c4", 4)
                                    .longColumn("c5", 5)
                                    .longColumn("c6", 6)
                                    .longColumn("c7", 7)
                                    .longColumn("c8", 8)
                                    .longColumn("c9", 9)
                                    .atNow(),
                            "c0", 0L,
                            "c1", 1L,
                            "c2", 2L,
                            "c3", 3L,
                            "c4", 4L,
                            "c5", 5L,
                            "c6", 6L,
                            "c7", 7L,
                            "c8", 8L,
                            "c9", 9L),
                    row("wide", sender -> sender.table("wide")
                                    .longColumn("c0", 10)
                                    .atNow(),
                            "c0", 10L,
                            "c1", Long.MIN_VALUE,
                            "c2", Long.MIN_VALUE,
                            "c3", Long.MIN_VALUE,
                            "c4", Long.MIN_VALUE,
                            "c5", Long.MIN_VALUE,
                            "c6", Long.MIN_VALUE,
                            "c7", Long.MIN_VALUE,
                            "c8", Long.MIN_VALUE,
                            "c9", Long.MIN_VALUE)
            );

            RunResult result = runScenario(rows, 0);

            Assert.assertEquals(1, result.sendCount);
            assertRowsEqual(expectedRows(rows), decodeRows(result.packets));
        });
    }

    @Test
    public void testUtf8StringAndSymbolStagingSupportsCancelAndPacketSizing() throws Exception {
        assertMemoryLeak(() -> {
            String msg1 = "Gruesse 東京";
            String msg2 = "Privet 👋 kosme";
            List<ScenarioRow> rows = Arrays.asList(
                    row("utf8", sender -> sender.table("utf8")
                                    .longColumn("x", 1)
                                    .symbol("sym", "東京")
                                    .stringColumn("msg", msg1)
                                    .atNow(),
                            "x", 1L,
                            "sym", "東京",
                            "msg", msg1),
                    row("utf8", sender -> sender.table("utf8")
                                    .longColumn("x", 2)
                                    .symbol("sym", "Αθηνα")
                                    .stringColumn("msg", msg2)
                                    .atNow(),
                            "x", 2L,
                            "sym", "Αθηνα",
                            "msg", msg2)
            );
            int maxDatagramSize = fullPacketSize(rows) - 1;

            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                sender.table("utf8")
                        .longColumn("x", 0)
                        .symbol("sym", "キャンセル")
                        .stringColumn("msg", "should not ship 👎");
                sender.cancelRow();

                sender.longColumn("x", 1)
                        .symbol("sym", "東京")
                        .stringColumn("msg", msg1)
                        .atNow();
                sender.longColumn("x", 2)
                        .symbol("sym", "Αθηνα")
                        .stringColumn("msg", msg2)
                        .atNow();
                sender.flush();
            }

            RunResult result = new RunResult(nf.packets, nf.lengths, nf.sendCount);
            Assert.assertTrue("expected at least one auto-flush", result.packets.size() > 1);
            assertPacketsWithinLimit(result, maxDatagramSize);
            assertRowsEqual(expectedRows(rows), decodeRows(nf.packets));
        });
    }

    @Test
    public void testNullArgsAfterCloseAllThrowSenderClosed() throws Exception {
        // Mirror of QwpWebSocketSenderTest's equivalent test: every column
        // setter that null-short-circuits or null-throws before delegating
        // must run checkNotClosed() first so a closed sender + null arg
        // produces the canonical "Sender is closed" error, not a
        // null-flavoured message (or a silent no-op).
        assertMemoryLeak(() -> {
            CapturingNetworkFacade nf = new CapturingNetworkFacade();
            QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024);
            sender.close();
            assertClosedUdp(() -> sender.decimalColumn("x", (Decimal64) null));
            assertClosedUdp(() -> sender.decimalColumn("x", (Decimal128) null));
            assertClosedUdp(() -> sender.decimalColumn("x", (Decimal256) null));
            assertClosedUdp(() -> sender.decimalColumn("x", (CharSequence) null));
            assertClosedUdp(() -> sender.doubleArray("x", (double[]) null));
            assertClosedUdp(() -> sender.doubleArray("x", (double[][]) null));
            assertClosedUdp(() -> sender.doubleArray("x", (double[][][]) null));
            assertClosedUdp(() -> sender.doubleArray("x", (DoubleArray) null));
            assertClosedUdp(() -> sender.geoHashColumn("x", null));
            assertClosedUdp(() -> sender.longArray("x", (long[]) null));
            assertClosedUdp(() -> sender.longArray("x", (long[][]) null));
            assertClosedUdp(() -> sender.longArray("x", (long[][][]) null));
            assertClosedUdp(() -> sender.longArray("x", (LongArray) null));
        });
    }

    private static void assertClosedUdp(Runnable r) {
        try {
            r.run();
            Assert.fail("Expected LineSenderException with 'closed' message");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "expected message to mention 'closed', got: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("closed"));
        }
    }

    private static void assertEstimateAtLeastActual(List<ScenarioRow> rows) {
        CapturingNetworkFacade nf = new CapturingNetworkFacade();
        try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, 1024 * 1024)) {
            for (int i = 0; i < rows.size(); i++) {
                rows.get(i).writer.accept(sender);
                long estimate = sender.committedDatagramEstimateForTest();
                long actual = fullPacketSize(rows.subList(0, i + 1));
                Assert.assertTrue(
                        "row " + i + " estimate underflow: estimate=" + estimate + ", actual=" + actual,
                        estimate >= actual
                );
            }
        }
    }

    private static void assertNullableArrayNullState(QwpTableBuffer.ColumnBuffer column) {
        Assert.assertNotNull(column);
        Assert.assertEquals(1, column.getSize());
        Assert.assertEquals(0, column.getValueCount());
        Assert.assertTrue(column.usesNullBitmap());
        Assert.assertTrue(column.isNull(0));
        Assert.assertEquals(0, column.getArrayShapeOffset());
        Assert.assertEquals(0, column.getArrayDataOffset());
    }

    private static void assertNullableStringNullState(QwpTableBuffer.ColumnBuffer column) {
        Assert.assertNotNull(column);
        Assert.assertEquals(1, column.getSize());
        Assert.assertEquals(0, column.getValueCount());
        Assert.assertTrue(column.usesNullBitmap());
        Assert.assertTrue(column.isNull(0));
        Assert.assertEquals(0, column.getStringDataSize());
        long offsetsAddress = column.getStringOffsetsAddress();
        Assert.assertTrue(offsetsAddress != 0);
        Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddress));
    }

    private static void assertPacketsWithinLimit(RunResult result, int maxDatagramSize) {
        for (int i = 0; i < result.lengths.size(); i++) {
            int len = result.lengths.get(i);
            Assert.assertTrue(
                    "packet " + i + " exceeds maxDatagramSize: " + len + " > " + maxDatagramSize,
                    len <= maxDatagramSize
            );
        }
    }

    private static void assertRowsEqual(List<DecodedRow> expected, List<DecodedRow> actual) {
        Assert.assertEquals("row count mismatch", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals("row mismatch at index " + i, expected.get(i), actual.get(i));
        }
    }

    private static void assertRowsEqualIgnoringOrder(List<DecodedRow> expected, List<DecodedRow> actual) {
        Map<DecodedRow, Integer> counts = new HashMap<>();
        for (DecodedRow row : expected) {
            counts.merge(row, 1, Integer::sum);
        }
        for (DecodedRow row : actual) {
            Integer count = counts.get(row);
            if (count == null) {
                Assert.fail("unexpected row: " + row);
            }
            if (count == 1) {
                counts.remove(row);
            } else {
                counts.put(row, count - 1);
            }
        }
        if (!counts.isEmpty()) {
            Assert.fail("missing rows: " + counts);
        }
    }

    private static void assertThrowsContains(String expected, ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail("Expected exception containing: " + expected);
        } catch (LineSenderException e) {
            Assert.assertTrue("Expected message to contain '" + expected + "' but got: " + e.getMessage(),
                    e.getMessage().contains(expected));
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception type", e);
        }
    }

    private static void auditEstimateAcrossSymbolDictionaryVarintBoundary() {
        ArrayList<ScenarioRow> rows = new ArrayList<>();
        for (int i = 0; i < 160; i++) {
            final int rowId = i;
            rows.add(row(
                    "sym_audit",
                    sender -> sender.table("sym_audit")
                            .longColumn("x", rowId)
                            .symbol("sym", "sym-" + rowId)
                            .atNow(),
                    "x", (long) rowId,
                    "sym", "sym-" + rowId
            ));
        }
        assertEstimateAtLeastActual(rows);
    }

    private static void auditEstimateWithStableSchemaAndNullableValues() {
        ArrayList<ScenarioRow> rows = new ArrayList<>();
        for (int i = 0; i < 96; i++) {
            final int rowId = i;
            final String stringValue = (i & 1) == 0 ? "tokyo-" + i + "-" + repeat('x', (i % 31) + 1) : null;
            final long[] longArray = i % 3 == 0 ? new long[]{i, i + 1L, i + 2L} : null;
            final double[][] doubleArray = i % 5 == 0 ? new double[][]{{i + 0.5, i + 1.5}, {i + 2.5, i + 3.5}} : null;
            final Decimal64 decimal64 = i % 7 == 0 ? Decimal64.fromLong(i * 100L + 7, 2) : null;
            final Decimal128 decimal128 = i % 11 == 0 ? Decimal128.fromLong(i * 1000L + 11, 4) : null;
            final Decimal256 decimal256 = i % 13 == 0 ? Decimal256.fromLong(i * 10000L + 13, 3) : null;

            rows.add(row(
                    "audit",
                    sender -> {
                        sender.table("audit")
                                .longColumn("l", rowId)
                                .doubleColumn("d", rowId + 0.25)
                                .symbol("sym", "stable");
                        if (stringValue != null) {
                            sender.stringColumn("s", stringValue);
                        }
                        if (longArray != null) {
                            sender.longArray("la", longArray);
                        }
                        if (doubleArray != null) {
                            sender.doubleArray("da", doubleArray);
                        }
                        if (decimal64 != null) {
                            sender.decimalColumn("d64", decimal64);
                        }
                        if (decimal128 != null) {
                            sender.decimalColumn("d128", decimal128);
                        }
                        if (decimal256 != null) {
                            sender.decimalColumn("d256", decimal256);
                        }
                        sender.at(rowId + 1L, ChronoUnit.MICROS);
                    },
                    "l", (long) rowId,
                    "d", rowId + 0.25,
                    "sym", "stable",
                    "s", stringValue,
                    "la", longArray == null ? null : longArrayValue(shape(longArray.length), longArray),
                    "da", doubleArray == null ? null : doubleArrayValue(shape(doubleArray.length, doubleArray[0].length), flatten(doubleArray)),
                    "d64", decimal64 == null ? null : decimal(i * 100L + 7, 2),
                    "d128", decimal128 == null ? null : decimal(i * 1000L + 11, 4),
                    "d256", decimal256 == null ? null : decimal(i * 10000L + 13, 3),
                    "", rowId + 1L
            ));
        }
        assertEstimateAtLeastActual(rows);
    }

    private static LinkedHashMap<String, Object> columns(Object... kvs) {
        if ((kvs.length & 1) != 0) {
            throw new IllegalArgumentException("key/value pairs expected");
        }
        LinkedHashMap<String, Object> columns = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            columns.put((String) kvs[i], kvs[i + 1]);
        }
        return columns;
    }

    private static BigDecimal decimal(long unscaled, int scale) {
        return BigDecimal.valueOf(unscaled, scale);
    }

    private static List<DecodedRow> decodeRows(List<byte[]> packets) {
        ArrayList<DecodedRow> rows = new ArrayList<>();
        for (byte[] packet : packets) {
            rows.addAll(new DatagramDecoder(packet).decode());
        }
        return rows;
    }

    private static DecodedRow decodedRow(String table, Object... kvs) {
        return new DecodedRow(table, columns(kvs));
    }

    private static DoubleArrayValue doubleArrayValue(int[] shape, double... values) {
        ArrayList<Integer> dims = new ArrayList<>(shape.length);
        for (int dim : shape) {
            dims.add(dim);
        }
        ArrayList<Double> elems = new ArrayList<>(values.length);
        for (double value : values) {
            elems.add(value);
        }
        return new DoubleArrayValue(dims, elems);
    }

    private static List<DecodedRow> expectedRows(List<ScenarioRow> rows) {
        ArrayList<DecodedRow> expected = new ArrayList<>(rows.size());
        for (ScenarioRow row : rows) {
            expected.add(row.expected);
        }
        return expected;
    }

    private static double[] flatten(double[][] matrix) {
        int size = 0;
        for (double[] row : matrix) {
            size += row.length;
        }
        double[] flat = new double[size];
        int offset = 0;
        for (double[] row : matrix) {
            System.arraycopy(row, 0, flat, offset, row.length);
            offset += row.length;
        }
        return flat;
    }

    private static long[] flatten(long[][] matrix) {
        int size = 0;
        for (long[] row : matrix) {
            size += row.length;
        }
        long[] flat = new long[size];
        int offset = 0;
        for (long[] row : matrix) {
            System.arraycopy(row, 0, flat, offset, row.length);
            offset += row.length;
        }
        return flat;
    }

    private static int fullPacketSize(List<ScenarioRow> rows) {
        RunResult result = runScenario(rows, 0);
        Assert.assertEquals("expected a single unbounded packet", 1, result.packets.size());
        return result.lengths.get(0);
    }

    private static LongArrayValue longArrayValue(int[] shape, long... values) {
        ArrayList<Integer> dims = new ArrayList<>(shape.length);
        for (int dim : shape) {
            dims.add(dim);
        }
        ArrayList<Long> elems = new ArrayList<>(values.length);
        for (long value : values) {
            elems.add(value);
        }
        return new LongArrayValue(dims, elems);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static ScenarioRow row(String table, Consumer<QwpUdpSender> writer, Object... kvs) {
        return new ScenarioRow(decodedRow(table, kvs), writer);
    }

    private static RunResult runScenario(List<ScenarioRow> rows, int maxDatagramSize) {
        CapturingNetworkFacade nf = new CapturingNetworkFacade();
        if (maxDatagramSize > 0) {
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1, maxDatagramSize)) {
                for (ScenarioRow row : rows) {
                    row.writer.accept(sender);
                }
                sender.flush();
            }
        } else {
            try (QwpUdpSender sender = new QwpUdpSender(nf, 0, 0, 9000, 1)) {
                for (ScenarioRow row : rows) {
                    row.writer.accept(sender);
                }
                sender.flush();
            }
        }
        return new RunResult(nf.packets, nf.lengths, nf.sendCount);
    }

    private static int[] shape(int... dims) {
        return dims;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class CapturingNetworkFacade extends NetworkFacadeImpl {
        private static final int SEGMENT_SIZE = 16;

        private final List<Integer> lengths = new ArrayList<>();
        private final List<byte[]> packets = new ArrayList<>();
        private final List<Integer> segmentCounts = new ArrayList<>();
        private int rawSendCount;
        private int scatterSendCount;
        private int sendCount;

        @Override
        public int close(int fd) {
            return 0;
        }

        @Override
        public void freeSockAddr(long pSockaddr) {
            // no-op
        }

        @Override
        public int sendToRaw(int fd, long lo, int len, long socketAddress) {
            byte[] packet = new byte[len];
            Unsafe.getUnsafe().copyMemory(null, lo, packet, Unsafe.BYTE_OFFSET, len);
            packets.add(packet);
            lengths.add(len);
            rawSendCount++;
            sendCount++;
            return len;
        }

        @Override
        public int sendToRawScatter(int fd, long segmentsPtr, int segmentCount, long socketAddress) {
            int packetLength = 0;
            long segmentPtr = segmentsPtr;
            for (int i = 0; i < segmentCount; i++) {
                packetLength += (int) Unsafe.getUnsafe().getLong(segmentPtr + 8);
                segmentPtr += SEGMENT_SIZE;
            }

            byte[] packet = new byte[packetLength];
            int offset = 0;
            segmentPtr = segmentsPtr;
            for (int i = 0; i < segmentCount; i++) {
                long dataAddress = Unsafe.getUnsafe().getLong(segmentPtr);
                int dataLength = (int) Unsafe.getUnsafe().getLong(segmentPtr + 8);
                Unsafe.getUnsafe().copyMemory(null, dataAddress, packet, Unsafe.BYTE_OFFSET + offset, dataLength);
                offset += dataLength;
                segmentPtr += SEGMENT_SIZE;
            }

            packets.add(packet);
            lengths.add(packetLength);
            segmentCounts.add(segmentCount);
            scatterSendCount++;
            sendCount++;
            return packetLength;
        }

        @Override
        public int setMulticastInterface(int fd, int ipv4Address) {
            return 0;
        }

        @Override
        public int setMulticastTtl(int fd, int ttl) {
            return 0;
        }

        @Override
        public long sockaddr(int address, int port) {
            return 1;
        }

        @Override
        public int socketUdp() {
            return 1;
        }
    }

    private static final class ColumnValues {
        private final Object[] rows;

        private ColumnValues(Object[] rows) {
            this.rows = rows;
        }
    }

    private static final class DatagramDecoder {
        private final PacketReader reader;

        private DatagramDecoder(byte[] packet) {
            this.reader = new PacketReader(packet);
        }

        private List<DecodedRow> decode() {
            Assert.assertEquals(MAGIC_MESSAGE, reader.readIntLE());
            Assert.assertEquals(VERSION, reader.readByte());
            reader.readByte();
            int tableCount = reader.readUnsignedShortLE();
            int payloadLength = reader.readIntLE();
            Assert.assertEquals(reader.length() - HEADER_SIZE, payloadLength);

            ArrayList<DecodedRow> rows = new ArrayList<>();
            for (int table = 0; table < tableCount; table++) {
                rows.addAll(decodeTable());
            }
            Assert.assertEquals(reader.length(), reader.position());
            return rows;
        }

        private void decodeBooleans(Object[] values, boolean[] nulls, int valueCount) {
            byte[] packed = reader.readBytes((valueCount + 7) / 8);
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    int byteIndex = valueIndex >>> 3;
                    int bitIndex = valueIndex & 7;
                    values[row] = (packed[byteIndex] & (1 << bitIndex)) != 0;
                    valueIndex++;
                }
            }
        }

        private ColumnValues decodeColumn(QwpColumnDef def, int rowCount) {
            boolean hasNullBitmap = reader.readByte() != 0;
            boolean[] nulls = hasNullBitmap ? reader.readNullBitmap(rowCount) : new boolean[rowCount];
            int nullCount = 0;
            for (boolean isNull : nulls) {
                if (isNull) {
                    nullCount++;
                }
            }
            int valueCount = rowCount - nullCount;
            Object[] values = new Object[rowCount];

            switch (def.getTypeCode()) {
                case TYPE_BOOLEAN:
                    decodeBooleans(values, nulls, valueCount);
                    break;
                case TYPE_DECIMAL64:
                    decodeDecimals(values, nulls, valueCount, 8);
                    break;
                case TYPE_DECIMAL128:
                    decodeDecimals(values, nulls, valueCount, 16);
                    break;
                case TYPE_DECIMAL256:
                    decodeDecimals(values, nulls, valueCount, 32);
                    break;
                case TYPE_DOUBLE:
                    decodeDoubles(values, nulls, valueCount);
                    break;
                case TYPE_DOUBLE_ARRAY:
                    decodeDoubleArrays(values, nulls, valueCount);
                    break;
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
                    decodeLongs(values, nulls, valueCount);
                    break;
                case TYPE_LONG_ARRAY:
                    decodeLongArrays(values, nulls, valueCount);
                    break;
                case TYPE_VARCHAR:
                    decodeStrings(values, nulls, valueCount);
                    break;
                case TYPE_SYMBOL:
                    decodeSymbols(values, nulls, valueCount);
                    break;
                default:
                    throw new AssertionError("Unsupported test decoder type: " + def.getTypeCode());
            }

            return new ColumnValues(values);
        }

        private void decodeDecimals(Object[] values, boolean[] nulls, int valueCount, int width) {
            int scale = reader.readByte();
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    byte[] bytes = reader.readBytes(width);
                    // LE wire format -> BE for BigInteger
                    for (int lo = 0, hi = bytes.length - 1; lo < hi; lo++, hi--) {
                        byte tmp = bytes[lo];
                        bytes[lo] = bytes[hi];
                        bytes[hi] = tmp;
                    }
                    values[row] = new BigDecimal(new BigInteger(bytes), scale);
                    valueIndex++;
                }
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeDoubleArrays(Object[] values, boolean[] nulls, int valueCount) {
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                    continue;
                }
                int dims = reader.readUnsignedByte();
                ArrayList<Integer> shape = new ArrayList<>(dims);
                int elementCount = 1;
                for (int d = 0; d < dims; d++) {
                    int dim = reader.readIntLE();
                    shape.add(dim);
                    elementCount = Math.multiplyExact(elementCount, dim);
                }
                ArrayList<Double> elements = new ArrayList<>(elementCount);
                for (int i = 0; i < elementCount; i++) {
                    elements.add(reader.readDoubleLE());
                }
                values[row] = new DoubleArrayValue(shape, elements);
                valueIndex++;
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeDoubles(Object[] values, boolean[] nulls, int valueCount) {
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    values[row] = reader.readDoubleLE();
                    valueIndex++;
                }
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeLongArrays(Object[] values, boolean[] nulls, int valueCount) {
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                    continue;
                }
                int dims = reader.readUnsignedByte();
                ArrayList<Integer> shape = new ArrayList<>(dims);
                int elementCount = 1;
                for (int d = 0; d < dims; d++) {
                    int dim = reader.readIntLE();
                    shape.add(dim);
                    elementCount = Math.multiplyExact(elementCount, dim);
                }
                ArrayList<Long> elements = new ArrayList<>(elementCount);
                for (int i = 0; i < elementCount; i++) {
                    elements.add(reader.readLongLE());
                }
                values[row] = new LongArrayValue(shape, elements);
                valueIndex++;
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeLongs(Object[] values, boolean[] nulls, int valueCount) {
            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    values[row] = reader.readLongLE();
                    valueIndex++;
                }
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeStrings(Object[] values, boolean[] nulls, int valueCount) {
            int[] offsets = new int[valueCount + 1];
            for (int i = 0; i <= valueCount; i++) {
                offsets[i] = reader.readIntLE();
            }
            byte[] data = reader.readBytes(offsets[valueCount]);

            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    int start = offsets[valueIndex];
                    int end = offsets[valueIndex + 1];
                    values[row] = new String(data, start, end - start, StandardCharsets.UTF_8);
                    valueIndex++;
                }
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private void decodeSymbols(Object[] values, boolean[] nulls, int valueCount) {
            int dictSize = (int) reader.readVarint();
            String[] dict = new String[dictSize];
            for (int i = 0; i < dictSize; i++) {
                dict[i] = reader.readString();
            }

            int valueIndex = 0;
            for (int row = 0; row < values.length; row++) {
                if (nulls[row]) {
                    values[row] = null;
                } else {
                    int index = (int) reader.readVarint();
                    values[row] = dict[index];
                    valueIndex++;
                }
            }
            Assert.assertEquals(valueCount, valueIndex);
        }

        private List<DecodedRow> decodeTable() {
            String tableName = reader.readString();
            int rowCount = (int) reader.readVarint();
            int columnCount = (int) reader.readVarint();

            QwpColumnDef[] defs = new QwpColumnDef[columnCount];
            for (int i = 0; i < columnCount; i++) {
                defs[i] = new QwpColumnDef(reader.readString(), reader.readByte());
            }

            ColumnValues[] columns = new ColumnValues[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columns[i] = decodeColumn(defs[i], rowCount);
            }

            ArrayList<DecodedRow> rows = new ArrayList<>(rowCount);
            for (int row = 0; row < rowCount; row++) {
                LinkedHashMap<String, Object> values = new LinkedHashMap<>();
                for (int col = 0; col < columnCount; col++) {
                    values.put(defs[col].getName(), columns[col].rows[row]);
                }
                rows.add(new DecodedRow(tableName, values));
            }
            return rows;
        }
    }

    private static final class DecodedRow {
        private final String table;
        private final LinkedHashMap<String, Object> values;

        private DecodedRow(String table, LinkedHashMap<String, Object> values) {
            this.table = table;
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DecodedRow that = (DecodedRow) o;
            return Objects.equals(table, that.table) && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, values);
        }

        @Override
        public String toString() {
            return "DecodedRow{" +
                    "table='" + table + '\'' +
                    ", values=" + values +
                    '}';
        }
    }

    private static final class DoubleArrayValue {
        private final List<Integer> shape;
        private final List<Double> values;

        private DoubleArrayValue(List<Integer> shape, List<Double> values) {
            this.shape = shape;
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleArrayValue that = (DoubleArrayValue) o;
            return Objects.equals(shape, that.shape) && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shape, values);
        }

        @Override
        public String toString() {
            return "DoubleArrayValue{" +
                    "shape=" + shape +
                    ", values=" + values +
                    '}';
        }
    }

    private static final class LongArrayValue {
        private final List<Integer> shape;
        private final List<Long> values;

        private LongArrayValue(List<Integer> shape, List<Long> values) {
            this.shape = shape;
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LongArrayValue that = (LongArrayValue) o;
            return Objects.equals(shape, that.shape) && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shape, values);
        }

        @Override
        public String toString() {
            return "LongArrayValue{" +
                    "shape=" + shape +
                    ", values=" + values +
                    '}';
        }
    }

    private static final class PacketReader {
        private final byte[] data;
        private int position;

        private PacketReader(byte[] data) {
            this.data = data;
        }

        private int length() {
            return data.length;
        }

        private int position() {
            return position;
        }

        private byte readByte() {
            return data[position++];
        }

        private byte[] readBytes(int len) {
            byte[] bytes = Arrays.copyOfRange(data, position, position + len);
            position += len;
            return bytes;
        }

        private double readDoubleLE() {
            double value = Unsafe.getUnsafe().getDouble(data, Unsafe.BYTE_OFFSET + position);
            position += Double.BYTES;
            return value;
        }

        private int readIntLE() {
            int value = Unsafe.byteArrayGetInt(data, position);
            position += Integer.BYTES;
            return value;
        }

        private long readLongLE() {
            long value = Unsafe.byteArrayGetLong(data, position);
            position += Long.BYTES;
            return value;
        }

        private boolean[] readNullBitmap(int rowCount) {
            byte[] bitmap = readBytes((rowCount + 7) / 8);
            boolean[] nulls = new boolean[rowCount];
            for (int row = 0; row < rowCount; row++) {
                int byteIndex = row >>> 3;
                int bitIndex = row & 7;
                nulls[row] = (bitmap[byteIndex] & (1 << bitIndex)) != 0;
            }
            return nulls;
        }

        private String readString() {
            int len = (int) readVarint();
            if (len == 0) {
                return "";
            }
            String value = new String(data, position, len, StandardCharsets.UTF_8);
            position += len;
            return value;
        }

        private int readUnsignedByte() {
            return readByte() & 0xff;
        }

        private int readUnsignedShortLE() {
            int value = Unsafe.byteArrayGetShort(data, position) & 0xffff;
            position += Short.BYTES;
            return value;
        }

        private long readVarint() {
            long value = 0;
            int shift = 0;
            while (true) {
                int b = readUnsignedByte();
                value |= (long) (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 63) {
                    throw new AssertionError("varint too long");
                }
            }
        }
    }

    private static final class RunResult {
        private final List<Integer> lengths;
        private final List<byte[]> packets;
        private final int sendCount;

        private RunResult(List<byte[]> packets, List<Integer> lengths, int sendCount) {
            this.packets = new ArrayList<>(packets);
            this.lengths = new ArrayList<>(lengths);
            this.sendCount = sendCount;
        }
    }

    private static final class ScenarioRow {
        private final DecodedRow expected;
        private final Consumer<QwpUdpSender> writer;

        private ScenarioRow(DecodedRow expected, Consumer<QwpUdpSender> writer) {
            this.expected = expected;
            this.writer = writer;
        }
    }
}
