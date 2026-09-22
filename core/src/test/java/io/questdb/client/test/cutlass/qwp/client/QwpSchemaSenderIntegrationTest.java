/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaSenderIntegrationTest {
    private static final String DECIMAL_TEXT_CORPUS =
            "/io/questdb/client/cutlass/qwp/native-decimal-to-text.tsv";
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testDecimalTextOverloadUsesTargetDecimalAndRecoversRows() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    921, 931, ColumnType.getDecimalType(5, 2));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events")
                        .decimalColumn("value", "1.20m")
                        .decimalColumn("value", "not-a-decimal")
                        .atNow();

                sender.uuidColumn("failed_b", 11, 12);
                try {
                    sender.decimalColumn("value", "not-a-decimal");
                    Assert.fail("expected invalid textual decimal");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }

                sender.decimalColumn("value", "2.5").atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrame()).decimal64Table(
                        "events", 921, 931, 2, 120, 250);
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDecimalTextOverloadFormatsTextAndPreservesNullNoOps() throws Exception {
        assertMemoryLeak(() -> {
            for (int targetType : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(941 + targetType, 951, targetType);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").decimalColumn("value", "00123.4500").atNow();
                    sender.decimalColumn("value", "1.2300m").atNow();
                    sender.decimalColumn("value", "Infinity")
                            .decimalColumn("value", "not-a-decimal")
                            .atNow();
                    sender.decimalColumn("value", (CharSequence) null)
                            .decimalColumn("value", "")
                            .atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).parsedDecimalTextTable(
                            "events", 941 + targetType, 951);
                }
            }
        });
    }

    @Test
    public void testSmallIntegerSettersUseExactNumericTargetWireAndRecoverRows() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {
                    ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT,
                    ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE
            };
            for (int i = 0; i < targets.length; i++) {
                int target = targets[i];
                LongTextSchemaHandler handler = new LongTextSchemaHandler(901 + i, 911 + i, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").byteColumn("value", (byte) -1).atNow();

                    sender.byteColumn("value", (byte) 99);
                    try {
                        switch (i % 3) {
                            case 0:
                                sender.byteColumn("failed_b", (byte) 1);
                                break;
                            case 1:
                                sender.shortColumn("failed_b", (short) 1);
                                break;
                            default:
                                sender.intColumn("failed_b", 1);
                                break;
                        }
                        Assert.fail("expected unsupported small-integer-to-UUID conversion");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.shortColumn("value", (short) 2).atNow();
                    sender.intColumn("value", Integer.MIN_VALUE).atNow();
                    sender.intColumn("value", 3).atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).smallIntegerNumericTable(
                            "events", 901 + i, 911 + i, numericWireType(target));
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                            handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testIntegerTemporalGenerationRemainsDateBeforeTimestampRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(981, 991, ColumnType.DATE);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").byteColumn("value", (byte) 11).atNow();

                handler.targetType = ColumnType.TIMESTAMP_NANO;
                handler.version = 992;
                sender.table("events");
                try {
                    sender.stringColumn("value", "1970-01-01T00:00:00.000001Z");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").intColumn("value", 12).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2))
                        .twoDateAndTimestampBlocks("events", 981, 991, 992);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testIntegerTemporalTargetsUseExactTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.DATE, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO};
            for (int i = 0; i < targets.length; i++) {
                int target = targets[i];
                LongTextSchemaHandler handler = new LongTextSchemaHandler(961 + i, 971 + i, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").byteColumn("value", (byte) -1).atNow();

                    sender.shortColumn("value", (short) 99);
                    try {
                        sender.intColumn("failed_b", 1);
                        Assert.fail("expected INT-to-UUID conversion rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.shortColumn("value", (short) 2).atNow();
                    sender.intColumn("value", Integer.MIN_VALUE).atNow();
                    sender.intColumn("value", 3).atNow();
                    sender.longColumn("value", Long.MAX_VALUE).atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).integerTemporalTable(
                            "events", 961 + i, 971 + i, temporalWireType(target));
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                            handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testSmallIntegerTextGenerationRemainsSymbolBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1021, 1031, ColumnType.SYMBOL);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").byteColumn("value", (byte) 7).atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 1032;
                sender.table("events");
                try {
                    sender.uuidColumn("value", 1, 2);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").shortColumn("value", (short) 8).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2))
                        .twoSymbolAndVarcharBlocks("events", 1021, 1031, 1032);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testSmallIntegerTextTargetsUseExactTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL};
            for (int i = 0; i < targets.length; i++) {
                int target = targets[i];
                LongTextSchemaHandler handler = new LongTextSchemaHandler(1041 + i, 1051 + i, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").byteColumn("value", (byte) -128).atNow();

                    sender.shortColumn("value", (short) 99);
                    try {
                        sender.intColumn("failed_b", 1);
                        Assert.fail("expected INT-to-UUID conversion rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.shortColumn("value", Short.MAX_VALUE).atNow();
                    sender.intColumn("value", Integer.MIN_VALUE).atNow();
                    sender.intColumn("value", Integer.MAX_VALUE).atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).smallIntegerTextTable(
                            "events", 1041 + i, 1051 + i, target);
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                            handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testSmallIntegerDecimalGenerationRemainsPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    1061, 1071, ColumnType.getDecimalType(3, 0));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").byteColumn("value", (byte) 1).atNow();

                handler.targetType = ColumnType.getDecimalType(20, 5);
                handler.version = 1072;
                sender.table("events");
                try {
                    sender.shortColumn("value", (short) 1_000);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").shortColumn("value", (short) 1_000).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoSmallIntegerDecimalBlocks(
                        "events", 1061, 1071, 1072);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testSmallIntegerDecimalTargetUsesExactWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    1081, 1091, ColumnType.getDecimalType(18, 2));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").byteColumn("value", (byte) 42).atNow();

                sender.shortColumn("value", (short) 99);
                try {
                    sender.intColumn("failed_b", 1);
                    Assert.fail("expected INT-to-UUID conversion rejection");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }

                sender.shortColumn("value", (short) -7).atNow();
                sender.intColumn("value", Integer.MIN_VALUE).atNow();
                sender.intColumn("value", Integer.MAX_VALUE).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrame()).smallIntegerDecimalTable(
                        "events", 1081, 1091);
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testIntToIpv4GenerationRemainsPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    1101, 1111, ColumnType.getDecimalType(3, 0));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").intColumn("value", 1).atNow();

                handler.targetType = ColumnType.IPv4;
                handler.version = 1112;
                sender.table("events");
                try {
                    sender.intColumn("value", 1_000);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").intColumn("value", 1_000).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoIntDecimalAndIpv4Blocks(
                        "events", 1101, 1111, 1112);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testIntToIpv4UsesExactTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1121, 1131, ColumnType.IPv4);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").intColumn("value", Integer.MIN_VALUE).atNow();
                sender.intColumn("value", 0).atNow();

                sender.intColumn("value", 0x0a000001);
                try {
                    sender.intColumn("failed_b", 1);
                    Assert.fail("expected INT to UUID rejection");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }

                sender.intColumn("value", 1).atNow();
                sender.intColumn("value", -1).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrame()).intToIpv4Table("events", 1121, 1131);
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testNativeDecimalTextCorpusUsesExactVarcharWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaSenderIntegrationTest.class.getResourceAsStream(DECIMAL_TEXT_CORPUS);
            Assert.assertNotNull(DECIMAL_TEXT_CORPUS, stream);
            boolean[][] sourceTargets = new boolean[3][2];
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    DecimalTextVector vector = new DecimalTextVector(line.split("\t", -1));
                    sourceTargets[vector.sourceIndex()][vector.targetIndex()] = true;
                    LongTextSchemaHandler handler = new LongTextSchemaHandler(
                            721 + count, 731 + count, vector.targetType());
                    try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                        sender.table("events");
                        vector.append(sender, "value");
                        if (vector.isNull()) {
                            sender.uuidColumn("failed_b", 11, 12);
                        }
                        sender.atNow();
                        sender.flush();
                        FrameReader reader = new FrameReader(handler.awaitDataFrame());
                        if (vector.isNull()) {
                            reader.uuidOnlyTable("events", 721 + count, 731 + count, 11, 12);
                        } else {
                            reader.decimalTextTable(
                                    "events", 721 + count, 731 + count, vector.expectedText);
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + vector.caseId + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(24, count);
            for (int source = 0; source < sourceTargets.length; source++) {
                for (int target = 0; target < sourceTargets[source].length; target++) {
                    Assert.assertTrue("missing source/target pair " + source + '/' + target,
                            sourceTargets[source][target]);
                }
            }
        });
    }

    @Test
    public void testNativeDecimalTextUsesPerValueScalesAcrossMixedWidthsAndRecoversRows() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(781, 791, ColumnType.VARCHAR);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events")
                        .decimalColumn("value", Decimal64.NULL_VALUE)
                        .decimalColumn("value", new Decimal64(12_340, 2))
                        .binaryColumn("value", new byte[]{1})
                        .atNow();

                sender.decimalColumn("value", Decimal256.MAX_VALUE);
                try {
                    sender.decimalColumn("failed_b", new Decimal64(1, 0));
                    Assert.fail("expected unsupported DECIMAL64-to-UUID conversion");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }

                sender.decimalColumn("value", new Decimal64(-125, 3)).atNow();
                sender.table("events").atNow();
                sender.decimalColumn("value", new Decimal256(
                        0x161bcca7119915b5L,
                        0x0764b4abe8652979L,
                        0x7775a5f171950fffL,
                        0xffffffffffffffffL,
                        76)).atNow();
                sender.decimalColumn("value", new Decimal64(0, 0)).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrame()).decimalTextLifecycleTable("events", 781, 791);
            }
        });
    }

    @Test
    public void testNativeDecimalTextMatchesServerScalarFormattingForRawPublicValues() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(801, 811, ColumnType.VARCHAR);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                Decimal64 wrappedScale = new Decimal64(123, 1);
                wrappedScale.setScale(257);
                sender.table("events")
                        .decimalColumn("value", new Decimal128(Long.MAX_VALUE, -1L))
                        .atNow();
                sender.decimalColumn("value", wrappedScale).atNow();
                sender.decimalColumn("value", new Decimal128(0, 420, 2)).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrame()).decimalTextTable(
                        "events",
                        801,
                        811,
                        "170141183460469231731687303715884105727",
                        "12.3",
                        "4.20"
                );
            }
        });
    }

    @Test
    public void testNativeDecimalRollbackAndTargetScaleWithoutTableReselection() throws Exception {
        assertMemoryLeak(() -> {
            int target = ColumnType.getDecimalType(16, 4);
            LongTextSchemaHandler handler = new LongTextSchemaHandler(601, 611, target);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").decimalColumn("value", new Decimal64(150, 2)).atNow();
                sender.uuidColumn("failed_b", 0xa456426614174000L, 0x123e4567e89b12d3L);
                try {
                    sender.decimalColumn("value", Decimal128.fromLong(100_000_000_000_000_000L, 0));
                    Assert.fail("expected target precision overflow");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                sender.decimalColumn("value", new Decimal64(25, 2)).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame()).decimal64Table("events", 601, 611, 4, 15_000, 2_500);
            }
        });
    }

    @Test
    public void testNarrowDecimalGenerationRemainsPinnedBeforeWideRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    621, 631, ColumnType.getDecimalType(3, 0));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").decimalColumn("value", Decimal128.fromLong(1, 0)).atNow();
                handler.targetType = ColumnType.getDecimalType(20, 0);
                handler.version = 632;
                sender.table("events");
                try {
                    sender.decimalColumn("value", Decimal128.fromLong(1_000, 0));
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").decimalColumn("value", Decimal128.fromLong(1_000, 0)).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimalBlocks("events", 621, 631, 632);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDecimalScaleGenerationRemainsPinnedBeforeScaleRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    641, 651, ColumnType.getDecimalType(18, 0));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").decimalColumn("value", new Decimal64(1, 0)).atNow();
                handler.targetType = ColumnType.getDecimalType(18, 4);
                handler.version = 652;
                sender.table("events");
                try {
                    sender.decimalColumn("value", new Decimal64(12_345, 4));
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").decimalColumn("value", new Decimal64(12_345, 4)).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimal64ScaleBlocks(
                        "events", 641, 651, 652);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testLongDecimalWidthAndScaleGenerationRemainPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    661, 671, ColumnType.getDecimalType(18, 4));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").longColumn("value", 1).atNow();
                handler.targetType = ColumnType.getDecimalType(20, 5);
                handler.version = 672;
                sender.table("events");
                try {
                    sender.longColumn("value", 100_000_000_000_000L);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").longColumn("value", 100_000_000_000_000L).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimalWidthAndScaleBlocks("events", 661, 671, 672);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testStringDecimalWidthAndScaleGenerationRemainPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    681, 691, ColumnType.getDecimalType(18, 4));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "1").atNow();
                handler.targetType = ColumnType.getDecimalType(20, 5);
                handler.version = 692;
                sender.table("events");
                try {
                    sender.stringColumn("value", "100000000000000");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("value", "100000000000000").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimalWidthAndScaleBlocks("events", 681, 691, 692);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testFloatingDecimalWidthAndScaleGenerationRemainPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    701, 711, ColumnType.getDecimalType(18, 4));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").floatColumn("value", 1.0f).atNow();
                handler.targetType = ColumnType.getDecimalType(20, 5);
                handler.version = 712;
                sender.table("events");
                try {
                    sender.doubleColumn("value", 100_000_000_000_000.0);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").doubleColumn("value", 100_000_000_000_000.0).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimalWidthAndScaleBlocks("events", 701, 711, 712);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testNativeDecimalTextGenerationRemainsPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    741, 751, ColumnType.getDecimalType(5, 4));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").decimalColumn("value", new Decimal64(12_340, 4)).atNow();
                handler.targetType = ColumnType.VARCHAR;
                handler.version = 752;
                sender.table("events");
                try {
                    sender.decimalColumn("value", new Decimal64(250_000, 4));
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").decimalColumn("value", new Decimal64(250_000, 4)).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDecimal64AndVarcharBlocks(
                        "events", 741, 751, 752);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testStringToGeoHashNullWidthsRollbackAndRecovery() throws Exception {
        assertMemoryLeak(() -> {
            for (int bits : new int[]{1, 8, 60}) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(
                        471 + bits, 481, ColumnType.getGeoHashTypeWithBits(bits));
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").stringColumn("value", null).atNow();
                    sender.flush();
                    new FrameReader(handler.awaitDataFrame()).nullGeoHashTable("events", 471 + bits, 481, bits);
                }
            }

            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    551, 561, ColumnType.getGeoHashTypeWithBits(8));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "zz").atNow();
                sender.stringColumn("failed_b", "123e4567-e89b-12d3-a456-426614174000");
                try {
                    sender.stringColumn("value", "a");
                    Assert.fail("expected invalid GeoHash text");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                sender.stringColumn("value", "04").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame()).geoHashTable("events", 551, 561, 8, 0xff, 1);
            }
        });
    }

    @Test
    public void testGeoHashPrecisionGenerationRemainsPinnedBeforeRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(
                    571, 581, ColumnType.getGeoHashTypeWithBits(60));
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "0123456789bc").atNow();
                handler.targetType = ColumnType.getGeoHashTypeWithBits(5);
                handler.version = 582;
                sender.table("events");
                try {
                    sender.stringColumn("value", "z");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("value", "z").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoGeoHashBlocks("events", 571, 581, 582);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testStringToLong256RollbackAndRecoveryWithoutTableReselection() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(431, 441, ColumnType.LONG256);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", null);
                sender.binaryColumn("value", new byte[]{1}).atNow();
                sender.stringColumn("failed_b", "123e4567-e89b-12d3-a456-426614174000");
                try {
                    sender.stringColumn("value", "0x0");
                    Assert.fail("expected odd-length LONG256 rejection");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                sender.stringColumn("value", "0x0123456789abcdeffedcba9876543210").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame()).long256Table("events", 431, 441);
            }
        });
    }

    @Test
    public void testLong256GenerationRemainsPinnedBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(451, 461, ColumnType.LONG256);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events")
                        .stringColumn("value", "0x0123456789abcdeffedcba9876543210")
                        .atNow();
                handler.targetType = ColumnType.VARCHAR;
                handler.version = 462;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-long256");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("value", "not-long256").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoLong256AndVarcharBlocks("events", 451, 461, 462);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testStringToCharRollbackAndRecoveryWithoutTableReselection() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(391, 401, ColumnType.CHAR);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "A").atNow();
                sender.uuidColumn("failed_b", 0xa456426614174000L, 0x123e4567e89b12d3L);
                try {
                    sender.binaryColumn("value", new byte[]{1});
                    Assert.fail("expected BINARY to CHAR rejection");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                sender.stringColumn("value", "\ud83d").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame()).charTable("events", 391, 401, 'A', '?');
            }
        });
    }

    @Test
    public void testVarcharGenerationRemainsPinnedBeforeCharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(411, 421, ColumnType.VARCHAR);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "native-a").atNow();
                handler.targetType = ColumnType.CHAR;
                handler.version = 422;
                sender.table("events");
                try {
                    sender.binaryColumn("value", new byte[]{1});
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").charColumn("value", '\uffff').atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoVarcharAndCharBlocks("events", 411, 421, 422);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testTimestampToTextTargetsRollbackAndRecoverWithoutTableReselection() throws Exception {
        assertMemoryLeak(() -> {
            for (int target : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(351 + target, 361, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").timestampColumn("value", Long.MIN_VALUE, ChronoUnit.MICROS).atNow();
                    sender.uuidColumn("failed_b", 0xa456426614174000L, 0x123e4567e89b12d3L);
                    try {
                        sender.timestampColumn("value", Long.MAX_VALUE, ChronoUnit.DAYS);
                        Assert.fail("expected timestamp range error");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                    }
                    sender.timestampColumn("value", 1, ChronoUnit.MILLIS).atNow();
                    sender.flush();
                    new FrameReader(handler.awaitDataFrame()).timestampTextTable(
                            "events", 351 + target, 361, "1970-01-01T00:00:00.001Z");
                }
            }
        });
    }

    @Test
    public void testNativeTimestampGenerationRemainsPinnedBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(371, 381, ColumnType.TIMESTAMP_NANO);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").timestampColumn("value", Long.MIN_VALUE, ChronoUnit.NANOS).atNow();
                handler.targetType = ColumnType.VARCHAR;
                handler.version = 382;
                sender.table("events");
                try {
                    sender.uuidColumn("value", 1, 0);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").timestampColumn("value", Long.MAX_VALUE, ChronoUnit.NANOS).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoTimestampAndVarcharBlocks(
                        "events", 371, 381, 382);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testStringToTimestampTargetsUsePinnedWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            for (int target : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(301 + target, 311, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").stringColumn("value", null);
                    sender.binaryColumn("value", new byte[]{1}).atNow();

                    sender.table("events").stringColumn("failed_b", "123e4567-e89b-12d3-a456-426614174000");
                    try {
                        sender.stringColumn("value", "not-a-timestamp");
                        Assert.fail("expected timestamp parse error");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                    }

                    sender.stringColumn("value", "1969-12-31T23:59:59.999999Z").atNow();
                    sender.flush();
                    new FrameReader(handler.awaitDataFrame()).longTimestampTable(
                            "events",
                            301 + target,
                            311,
                            target,
                            target == ColumnType.TIMESTAMP_MICRO ? -1 : -1000
                    );
                }
            }
        });
    }

    @Test
    public void testStringToDateUsesExactTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1141, 1151, ColumnType.DATE);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", null);
                sender.binaryColumn("value", new byte[]{1}).atNow();

                sender.table("events").stringColumn("failed_b", "123e4567-e89b-12d3-a456-426614174000");
                try {
                    sender.stringColumn("value", "not-a-date");
                    Assert.fail("expected DATE parse error");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }

                sender.stringColumn("value", "1969-12-31").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame())
                        .stringDateTable("events", 1141, 1151, -86_400_000L);
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDateGenerationRemainsPinnedBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1161, 1171, ColumnType.DATE);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "1970-01-01").atNow();
                handler.targetType = ColumnType.VARCHAR;
                handler.version = 1172;
                sender.table("events");
                try {
                    sender.stringColumn("value", "native-a");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("value", "native-a").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2))
                        .twoDateAndVarcharBlocks("events", 1161, 1171, 1172);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDoubleArrayUsesExactTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int array2d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1181, 1191, array2d);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").doubleArray("value", new double[][]{{1.0, 2.0}}).atNow();

                sender.table("events").uuidColumn("failed_b", 11, 12);
                try {
                    sender.doubleArray("value", new double[]{99.0});
                    Assert.fail("expected array rank mismatch");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }

                sender.doubleArray("value", new double[][]{{3.0}, {4.0}}).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame())
                        .doubleArrayTable("events", 1181, 1191);
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDoubleArrayGenerationRemainsPinnedAcrossRankChange() throws Exception {
        assertMemoryLeak(() -> {
            int array2d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            int array1d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1201, 1211, array2d);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").doubleArray("value", new double[][]{{1.0, 2.0}}).atNow();
                handler.targetType = array1d;
                handler.version = 1212;
                sender.table("events");
                try {
                    sender.doubleArray("value", new double[]{3.0, 4.0});
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").doubleArray("value", new double[]{3.0, 4.0}).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2))
                        .twoDoubleArrayRankBlocks("events", 1201, 1211, 1212);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testLongArrayRejectionRollsBackWithoutRefresh() throws Exception {
        assertMemoryLeak(() -> {
            for (int overload = 0; overload < 4; overload++) {
                int array2d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
                LongTextSchemaHandler handler = new LongTextSchemaHandler(1221, 1231, array2d);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").doubleArray("value", new double[][]{{1.0, 2.0}}).atNow();

                    sender.table("events").uuidColumn("failed_b", 11, 12);
                    try {
                        switch (overload) {
                            case 0:
                                sender.longArray("value", new long[]{99L});
                                break;
                            case 1:
                                sender.longArray("value", new long[][]{{99L}});
                                break;
                            case 2:
                                sender.longArray("value", new long[][][]{{{99L}}});
                                break;
                            default:
                                try (LongArray array = new LongArray(1)) {
                                    sender.longArray("value", array.append(99L));
                                }
                                break;
                        }
                        Assert.fail("expected unsupported LONG_ARRAY setter [overload=" + overload + "]");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.doubleArray("value", new double[][]{{3.0}, {4.0}}).atNow();
                    sender.flush();
                    // Only the two completed DOUBLE[] rows reach the wire; failed_b is rolled back.
                    new FrameReader(handler.awaitDataFrame())
                            .doubleArrayTable("events", 1221, 1231);
                    Assert.assertEquals("unsupported input must not refresh [overload=" + overload + "]", 1,
                            handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testLongArrayRejectionKeepsPinnedSnapshotAfterSchemaChange() throws Exception {
        assertMemoryLeak(() -> {
            int array2d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            int array1d = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            LongTextSchemaHandler handler = new LongTextSchemaHandler(1241, 1251, array2d);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").doubleArray("value", new double[][]{{1.0, 2.0}}).atNow();
                handler.targetType = array1d;
                handler.version = 1252;
                for (int attempt = 0; attempt < 2; attempt++) {
                    sender.table("events").uuidColumn("failed_b", 11, 12);
                    try {
                        sender.longArray("value", new long[]{3L, 4L});
                        Assert.fail("expected unsupported LONG_ARRAY setter");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }
                    Assert.assertEquals("schema changes cannot implement LONG_ARRAY", 1,
                            handler.describeRequests.get());
                }
                // A supported input of the new rank is still rejected against the
                // pinned snapshot; the batch does not discover the rank change.
                try {
                    sender.doubleArray("value", new double[]{3.0, 4.0});
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").doubleArray("value", new double[]{3.0, 4.0}).atNow();
                sender.flush();
                // The rebound block holds the recovered row alone: rollback dropped
                // failed_b with the rejected row.
                new FrameReader(handler.awaitDataFrames(2))
                        .twoDoubleArrayRankBlocks("events", 1241, 1251, 1252);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test(timeout = 10_000)
    public void testLongArrayRejectionDoesNotDependOnDescribeAvailability() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 91, 92);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";schema_wait_millis=100;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                         + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                handler.respondToDescribe = false;
                sender.table("events").stringColumn("partial", "discard");
                try {
                    sender.longArray("id", new long[]{99L});
                    Assert.fail("expected unsupported LONG_ARRAY setter");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testVarcharGenerationRemainsPinnedBeforeTimestampRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(321, 331, ColumnType.VARCHAR);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").stringColumn("value", "native-a").atNow();
                handler.targetType = ColumnType.TIMESTAMP_MICRO;
                handler.version = 332;
                sender.table("events");
                try {
                    sender.stringColumn("failed_b", "not-a-uuid");
                    Assert.fail("expected invalid UUID");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("value", "1970-01-01T00:00:00.000001Z").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoVarcharAndTimestampBlocks(
                        "events", 321, 331, 332
                );
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testLongToTimestampTargetsUseRawUnitsAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            for (int target : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(251 + target, 261, target);
                try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                    sender.table("events").longColumn("value", Long.MIN_VALUE);
                    sender.binaryColumn("value", new byte[]{1}).atNow();
                    sender.table("events").longColumn("value", -1);
                    try {
                        sender.longColumn("failed_b", 9);
                        Assert.fail("expected LONG to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }
                    sender.longColumn("value", Long.MAX_VALUE).atNow();
                    sender.flush();
                    new FrameReader(handler.awaitDataFrame())
                            .longTimestampTable("events", 251 + target, 261, target, Long.MAX_VALUE);
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testLongGenerationRemainsNativeBeforeTimestampRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(271, 281, ColumnType.LONG);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").longColumn("value", 11).atNow();
                handler.targetType = ColumnType.TIMESTAMP_NANO;
                handler.version = 282;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-a-long");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").longColumn("value", 12).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoLongAndTimestampBlocks("events", 271, 281, 282);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDoubleGenerationRemainsNativeBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(231, 241, ColumnType.DOUBLE);
            try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                sender.table("events").doubleColumn("value", -0.0).atNow();
                handler.targetType = ColumnType.VARCHAR;
                handler.version = 242;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-a-double");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").doubleColumn("value", 1e23).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2)).twoDoubleAndVarcharBlocks("events", 231, 241, 242);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testFloatingToTextTargetsUsePinnedWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            for (String input : new String[]{"FLOAT", "DOUBLE"}) {
                for (int target : new int[]{ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL}) {
                    LongTextSchemaHandler handler = new LongTextSchemaHandler(211 + target, 221, target);
                    try (TestWebSocketServer server = schemaServer(handler); Sender sender = sender(server)) {
                        sender.table("events");
                        if ("FLOAT".equals(input)) sender.floatColumn("value", Float.intBitsToFloat(0x7fc00002));
                        else sender.doubleColumn("value", Double.longBitsToDouble(0x7ff8000000000002L));
                        sender.binaryColumn("value", new byte[]{1}).atNow();

                        sender.table("events");
                        if ("FLOAT".equals(input)) sender.floatColumn("value", 0.1f);
                        else sender.doubleColumn("value", 1e23);
                        try {
                            sender.longColumn("failed_b", 9);
                            Assert.fail("expected floating row rejection");
                        } catch (LineSenderSchemaException e) {
                            Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                        }

                        if ("FLOAT".equals(input)) sender.floatColumn("value", Float.intBitsToFloat(1));
                        else sender.doubleColumn("value", Double.longBitsToDouble(1));
                        sender.atNow();
                        sender.flush();

                        new FrameReader(handler.awaitDataFrame()).floatingTextTable(
                                "events", 211 + target, 221, target,
                                "FLOAT".equals(input) ? "0.10000000149011612" : "1.0E23",
                                "FLOAT".equals(input) ? "1.401298464324817E-45" : "5.0E-324"
                        );
                        Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                    }
                }
            }
        });
    }

    @Test
    public void testUuidGenerationRemainsNativeBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(191, 201, ColumnType.UUID);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events")
                        .uuidColumn("value", 0x0102030405060708L, 0x1112131415161718L)
                        .atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 202;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-a-uuid");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").uuidColumn("value", 0x2122232425262728L, 0x3132333435363738L).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoUuidAndVarcharBlocks("events", 191, 201, 202);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testUuidToTextTargetsUsePinnedTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.STRING, ColumnType.VARCHAR};
            for (int target : targets) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(171 + target, 181, target);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events").uuidColumn("value", Long.MIN_VALUE, Long.MIN_VALUE);
                    sender.binaryColumn("value", new byte[]{1}).atNow();

                    sender.table("events").uuidColumn("value", 7, 0);
                    try {
                        sender.longColumn("failed_b", 9);
                        Assert.fail("expected LONG to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.uuidColumn("value", 0x0102030405060708L, 0x1112131415161718L).atNow();
                    sender.flush();

                    FrameReader reader = new FrameReader(handler.awaitDataFrame());
                    reader.longTextTable(
                            "events",
                            171 + target,
                            181,
                            target,
                            "11121314-1516-1718-0102-030405060708"
                    );
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testIpv4TargetsUsePinnedTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.IPv4, ColumnType.STRING, ColumnType.VARCHAR};
            for (int target : targets) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(211 + target, 221, target);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events")
                            .ipv4Column("value", 0)
                            .ipv4Column("value", "not-an-ip")
                            .atNow();

                    sender.table("events").ipv4Column("value", 0x05060708);
                    try {
                        sender.ipv4Column("failed_b", 0x090a0b0c);
                        Assert.fail("expected IPv4 to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.uuidColumn("failed_b", 1, 2);
                    try {
                        sender.ipv4Column("value", "not-an-ip");
                        Assert.fail("expected invalid IPv4 text");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                    }

                    sender.ipv4Column("value", ".....255.1.2.3......").atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).ipv4Table(
                            "events", 211 + target, 221, target, 0xff010203);
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testIpv4GenerationRemainsNativeBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(231, 241, ColumnType.IPv4);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").ipv4Column("value", 0x01020304).atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 242;
                sender.table("events");
                try {
                    sender.ipv4Column("value", "not-an-ip");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").ipv4Column("value", 0x05060708).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoIpv4AndVarcharBlocks("events", 231, 241, 242);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testGeoHashTargetsUsePinnedTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int geo20 = ColumnType.getGeoHashTypeWithBits(20);
            int[] targets = {geo20, ColumnType.STRING, ColumnType.VARCHAR};
            for (int target : targets) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(251 + target, 261, target);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events")
                            .geoHashColumn("value", "u33d")
                            .atNow();

                    sender.table("events").geoHashColumn("value", 0x11111, 20);
                    try {
                        sender.geoHashColumn("failed_b", 1, 20);
                        Assert.fail("expected GEOHASH to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.geoHashColumn("value", 0x12345, 20).atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).geoHashTargetTable(
                            "events", 251 + target, 261, target, 0xd0c6c, 0x12345, 20);
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testGeoHashGenerationRemainsNativeBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            int geo20 = ColumnType.getGeoHashTypeWithBits(20);
            LongTextSchemaHandler handler = new LongTextSchemaHandler(281, 291, geo20);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").geoHashColumn("value", 0xabcde, 20).atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 292;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-geohash");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").geoHashColumn("value", 0x12345, 20).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoNativeGeoHashAndVarcharBlocks(
                        "events", 281, 291, 292);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testLong256TargetsUsePinnedTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.LONG256, ColumnType.STRING, ColumnType.VARCHAR};
            for (int target : targets) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(251 + target, 261, target);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events")
                            .long256Column("value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)
                            .long256Column("value", 9, 10, 11, 12)
                            .atNow();

                    sender.table("events").long256Column("value", 5, 6, 7, 8);
                    try {
                        sender.long256Column("failed_b", 9, 10, 11, 12);
                        Assert.fail("expected LONG256 to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    sender.long256Column("value", 1, 2, 3, 4).atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).long256TargetTable(
                            "events", 251 + target, 261, target, 1, 2, 3, 4);
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testLong256GenerationRemainsNativeBeforeVarcharRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(281, 291, ColumnType.LONG256);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").long256Column("value", 1, 2, 3, 4).atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 292;
                sender.table("events");
                try {
                    sender.stringColumn("value", "not-long256");
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").long256Column("value", 5, 6, 7, 8).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2)).twoNativeLong256AndVarcharBlocks(
                        "events", 281, 291, 292);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testLongToTextTargetsUsePinnedTargetWireAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            int[] targets = {ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL};
            for (int target : targets) {
                LongTextSchemaHandler handler = new LongTextSchemaHandler(121 + target, 131, target);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events").longColumn("value", Long.MIN_VALUE);
                    // MIN is an effective null write, so this unsupported duplicate must be ignored.
                    sender.binaryColumn("value", new byte[]{1}).atNow();

                    sender.table("events").longColumn("value", 7);
                    try {
                        sender.longColumn("failed_b", 9);
                        Assert.fail("expected LONG to UUID rejection");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }

                    // The failed middle row is auto-cancelled; continue without selecting the table again.
                    sender.longColumn("value", Long.MAX_VALUE).atNow();
                    sender.flush();

                    FrameReader reader = new FrameReader(handler.awaitDataFrame());
                    reader.longTextTable("events", 121 + target, 131, target, Long.toString(Long.MAX_VALUE));
                    Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testLongSymbolGenerationRemainsPinnedAcrossTextRebind() throws Exception {
        assertMemoryLeak(() -> {
            LongTextSchemaHandler handler = new LongTextSchemaHandler(151, 161, ColumnType.SYMBOL);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").longColumn("value", 7).atNow();

                handler.targetType = ColumnType.VARCHAR;
                handler.version = 162;
                sender.table("events");
                try {
                    sender.uuidColumn("value", 1, 2);
                    Assert.fail("expected rejection against the pinned snapshot");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                    handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").longColumn("value", 8).atNow();
                sender.flush();

                new FrameReader(handler.awaitDataFrames(2))
                        .twoSymbolAndVarcharBlocks("events", 151, 161, 162);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testAsymmetricUuidRowsArePinnedAndInvalidMiddleRowRollsBack() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 41, 73);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").stringColumn("id", "11121314-1516-1718-0102-030405060708").atNow();
                sender.table("events");
                try {
                    sender.stringColumn("failed_b", "must-roll-back");
                    sender.stringColumn("id", "not-a-uuid");
                    Assert.fail("expected invalid UUID");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                sender.uuidColumn("id", 0x2122232425262728L, 0x3132333435363738L).atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                FrameReader reader = new FrameReader(frame);
                reader.schemaTable("events", 41, 73, 2, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0x0102030405060708L, reader.i64());
                Assert.assertEquals(0x1112131415161718L, reader.i64());
                Assert.assertEquals(0x2122232425262728L, reader.i64());
                Assert.assertEquals(0x3132333435363738L, reader.i64());
                reader.eof();
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testDuplicateSetterSkipsInvalidConversionAndRefresh() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 7, 9);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").uuidColumn("id", 1, 2);
                sender.stringColumn("id", "invalid and ignored").atNow();
                sender.flush();
                Assert.assertNotNull(handler.awaitDataFrame());
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testExplicitAndServerAssignedDesignatedTimestampUseSchemaFrames() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 11, 12);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").uuidColumn("id", 1, 2).at(Instant.ofEpochSecond(3, 4));
                sender.table("events").uuidColumn("id", 5, 6).atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                        frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
            }
        });
    }

    @Test
    public void testMissingTableInfersColumnsAndUsesCachedAbsence() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_MISSING, -1, -1);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("missing").longColumn("x", 7).atNow();
                sender.table("missing").longColumn("x", 8).atNow();
                sender.flush();
                FrameReader reader = new FrameReader(handler.awaitDataFrame());
                reader.schemaTable("missing", -1, -1, 2, "x", QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(7, reader.i64());
                Assert.assertEquals(8, reader.i64());
                reader.eof();
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testKnownAdoptionWaitsForBatchBoundary() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_MISSING, 31, 41);
            handler.isSchemaFeedbackEnabled = true;
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").longColumn("id", 11).atNow();
                handler.result = QwpSchemaProtocol.RESULT_KNOWN;
                // The pending batch keeps its inferred snapshot: a conflicting input
                // fails against the inferred LONG and the failed row rolls back whole.
                try {
                    sender.stringColumn("failed_b", "partial");
                    sender.stringColumn("id", "11121314-1516-1718-0102-030405060708");
                    Assert.fail("expected inferred type conflict");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals(1, handler.describeRequests.get());
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").stringColumn("id", "21222324-2526-2728-3132-333435363738").atNow();
                sender.flush();
                FrameReader reader = new FrameReader(handler.awaitDataFrames(2));
                reader.twoInferredAdoptionBlocks("events");
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testInferredTypeConflictDoesNotRefreshAndRetainsInferredRows() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_MISSING, -1, -1);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").longColumn("id", 1).atNow();
                try {
                    sender.stringColumn("id", "different inferred type");
                    Assert.fail("expected inferred type conflict");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                sender.longColumn("id", 3).atNow();
                sender.flush();
                FrameReader reader = new FrameReader(handler.awaitDataFrame());
                reader.schemaTable("events", -1, -1, 2, "id", QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(3, reader.i64());
                reader.eof();
                Assert.assertEquals("a rejected setter must not look up metadata", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testAutoUnavailableSchemaUsesLegacyFrame() throws Exception {
        assertMemoryLeak(() -> {
            for (int result : new int[]{QwpSchemaProtocol.RESULT_UNAVAILABLE, QwpSchemaProtocol.RESULT_TOO_LARGE}) {
                SchemaHandler handler = new SchemaHandler(result, -1, -1);
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = sender(server)) {
                    sender.table("events").stringColumn("value", "legacy").atNow();
                    sender.flush();

                    new FrameReader(handler.awaitDataFrame()).legacyVarcharTable("events", "value", "legacy");
                }
            }
        });
    }

    @Test
    public void testAutoTooLargeSchemaPublishesPendingSchemaBatchBeforeLegacyRow() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 121, 122);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                handler.result = QwpSchemaProtocol.RESULT_TOO_LARGE;

                // The new table cannot join the pending schema batch: publish it
                // before the legacy row starts a new frame.
                sender.table("other").stringColumn("legacy_value", "A").atNow();
                sender.flush();

                List<byte[]> frames = handler.awaitDataFrames(2);
                FrameReader schema = new FrameReader(frames.get(0));
                schema.schemaTable("events", 121, 122, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, schema.u8());
                Assert.assertEquals(1, schema.i64());
                Assert.assertEquals(2, schema.i64());
                schema.eof();
                new FrameReader(frames.get(1)).legacyVarcharTable("other", "legacy_value", "A");
            }
        });
    }

    @Test
    public void testStrictInvalidateAllFeedbackMakesNextBatchLookUpAndSurfaceFailure() throws Exception {
        assertMemoryLeak(() -> {
            int[] results = {
                    QwpSchemaProtocol.RESULT_DENIED,
                    QwpSchemaProtocol.RESULT_UNAVAILABLE,
                    QwpSchemaProtocol.RESULT_TOO_LARGE
            };
            LineSenderSchemaException.Reason[] reasons = {
                    LineSenderSchemaException.Reason.ACCESS_DENIED,
                    LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE,
                    LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE
            };
            for (int i = 0; i < results.length; i++) {
                SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_MISSING, -1, -1);
                handler.isSchemaFeedbackEnabled = true;
                // STRICT surfaces every lookup failure; AUTO would write the
                // RESULT_UNAVAILABLE and RESULT_TOO_LARGE cases with the legacy contract instead.
                try (TestWebSocketServer server = schemaServer(handler);
                     Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                             + ";schema_mode=strict;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                             + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                    sender.table("events").longColumn("id", 1).atNow();
                    handler.result = results[i];
                    // The pending batch never consults the server: the conflict is
                    // local and only the partial row rolls back.
                    try {
                        sender.stringColumn("failed_only", "partial");
                        sender.stringColumn("id", "different inferred type");
                        Assert.fail("expected inferred type conflict");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                    }
                    sender.longColumn("id", 3).atNow();
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                    FrameReader reader = new FrameReader(handler.awaitDataFrame());
                    reader.schemaTable("events", -1, -1, 2, "id", QwpConstants.TYPE_LONG);
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(1, reader.i64());
                    Assert.assertEquals(3, reader.i64());
                    reader.eof();
                    Assert.assertEquals(1, handler.describeRequests.get());

                    // The server could not describe the table for the stale frame, so
                    // its ACK invalidated the cache. The next explicit table selection
                    // looks the table up and reports the failure there.
                    try {
                        sender.table("events");
                        Assert.fail("expected lookup failure");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(reasons[i], e.getReason());
                    }
                    Assert.assertEquals(2, handler.describeRequests.get());
                    handler.result = QwpSchemaProtocol.RESULT_MISSING;
                    sender.table("events").longColumn("id", 5).atNow();
                    sender.flush();
                    reader = new FrameReader(handler.awaitDataFrames(2).get(1));
                    reader.schemaTable("events", -1, -1, 1, "id", QwpConstants.TYPE_LONG);
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(5, reader.i64());
                    reader.eof();
                    Assert.assertEquals(3, handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testTableNegotiatesBeforeNullNoops() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").doubleArray("values", (double[]) null);
                sender.longArray("values", (long[]) null);
                sender.longArray("values", (long[][]) null);
                sender.longArray("values", (long[][][]) null);
                sender.longArray("values", (LongArray) null);
                sender.ipv4Column("address", (CharSequence) null);
                Assert.assertEquals(1, handler.describeRequests.get());
                sender.cancelRow();
            }
        });
    }

    @Test
    public void testUnsupportedSetterCannotBypassSchemaAndInvalidNameDoesNotRefresh() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 2, 3);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events");
                try {
                    sender.longColumn("id", 1);
                    Assert.fail("expected UUID target rejection");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, e.getReason());
                }
                Assert.assertEquals(1, handler.describeRequests.get());
                sender.table("events");
                try {
                    sender.stringColumn("bad\nname", "x");
                    Assert.fail("expected invalid column name");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("column name"));
                }
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testOldPeerUsesLegacyFrameAndNeverDescribes() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isStrict : new boolean[]{false, true}) {
                SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
                try (TestWebSocketServer server = legacyServer(handler);
                     Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                             + ";schema_mode=" + (isStrict ? "strict" : "auto") + ";close_flush_timeout_millis=0;")) {
                    sender.table("events").stringColumn("value", "legacy").atNow();
                    sender.flush();
                    byte[] frame = handler.awaitDataFrame();
                    Assert.assertEquals(0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                    FrameReader reader = new FrameReader(frame);
                    reader.legacyVarcharTable("events", "value", "legacy");
                    Assert.assertEquals(0, handler.describeRequests.get());
                }
            }
        });
    }

    @Test
    public void testOldPeerUsesLegacySmallIntegerWireAndNeverDescribes() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
            try (TestWebSocketServer server = legacyServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events")
                        .byteColumn("b", (byte) -7)
                        .shortColumn("s", (short) 42)
                        .intColumn("i", Integer.MIN_VALUE)
                        .atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                Assert.assertEquals(0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                new FrameReader(frame).legacySmallIntegerTable("events");
                Assert.assertEquals(0, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testOldPeerUsesLegacyIpv4WireAndNeverDescribes() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
            try (TestWebSocketServer server = legacyServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").ipv4Column("value", 0xc0a80101).atNow();
                sender.table("events").ipv4Column("value", "10.20.30.40").atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                Assert.assertEquals(0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                new FrameReader(frame).legacyIpv4Table(
                        "events", "value", 0xc0a80101, 0x0a141e28);
                Assert.assertEquals(0, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testOldPeerUsesLegacyLong256WireAndNeverDescribes() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
            try (TestWebSocketServer server = legacyServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").long256Column("value", 1, 2, 3, 4).atNow();
                sender.table("events").long256Column(
                        "value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE).atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                Assert.assertEquals(0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                new FrameReader(frame).legacyLong256Table("events", "value");
                Assert.assertEquals(0, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testOldPeerUsesLegacyGeoHashWireAndNeverDescribes() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
            try (TestWebSocketServer server = legacyServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").geoHashColumn("value", 3, 5).atNow();
                sender.table("events").geoHashColumn("value", "U").atNow();
                sender.flush();
                byte[] frame = handler.awaitDataFrame();
                Assert.assertEquals(0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                new FrameReader(frame).legacyGeoHashTable("events", "value");
                Assert.assertEquals(0, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testVersionChangeDoesNotSplitPendingBatch() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 51, 52);
            handler.isSchemaFeedbackEnabled = true;
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                handler.version = 53;
                sender.table("events");
                try {
                    sender.stringColumn("id", "still-invalid");
                    Assert.fail("expected UUID validation error");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                // The rejection neither looks up metadata nor splits the batch: the
                // next row joins the block pinned to version 52.
                sender.uuidColumn("id", 3, 4).atNow();
                long fsn = sender.flushAndGetSequence();
                new FrameReader(handler.awaitDataFrame()).uuidTable("events", 51, 52, 1, 2, 3, 4);
                Assert.assertEquals(1, handler.describeRequests.get());

                // The stale frame's ACK carries version 53; the next batch adopts it.
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").uuidColumn("id", 5, 6).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2).get(1)).uuidTable("events", 51, 53, 5, 6);
                Assert.assertEquals("the ACK feedback replaces a second lookup", 1,
                        handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testPendingLegacyBatchStaysLegacyAcrossUpgradeAndNextBatchAdoptsSchema() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isStrict : new boolean[]{false, true}) {
                int port = TestPorts.findUnusedPort();
                SchemaHandler legacyHandler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 1, 1);
                TestWebSocketServer legacy = new TestWebSocketServer(legacyHandler, false, null, port);
                legacy.start();
                Assert.assertTrue(legacy.awaitStart(5, TimeUnit.SECONDS));
                CountDownLatch disconnected = new CountDownLatch(1);
                Sender sender = Sender.builder("ws::addr=localhost:" + port
                        + ";schema_mode=" + (isStrict ? "strict" : "auto") + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;"
                        + "close_flush_timeout_millis=0;")
                        .connectionListener(event -> {
                            if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                                disconnected.countDown();
                            }
                        })
                        .build();
                TestWebSocketServer upgraded = null;
                try {
                    sender.table("events").stringColumn("legacy_value", "A").atNow();
                    legacy.close();
                    Assert.assertTrue("sender did not observe the legacy connection closing",
                            disconnected.await(5, TimeUnit.SECONDS));
                    sender.table("events").stringColumn("legacy_value", "B");
                    SchemaHandler schemaHandler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 61, 62);
                    upgraded = new TestWebSocketServer(schemaHandler, false, null, port);
                    upgraded.setAdvertiseSchema(true);
                    upgraded.start();
                    Assert.assertTrue(upgraded.awaitStart(5, TimeUnit.SECONDS));
                    Assert.assertTrue("schema-capable connection was not installed and servicing frames",
                            schemaHandler.awaitPong());
                    sender.atNow();
                    // A batch has one wire contract: the pending batch already holds
                    // legacy rows, so this row stays legacy although the supporting
                    // connection is up. The first row after the flush adopts schema mode.
                    sender.table("events").stringColumn("legacy_value", "C").atNow();
                    sender.flush();
                    sender.table("events").uuidColumn("id", 3, 4).atNow();
                    sender.flush();
                    List<byte[]> frames = schemaHandler.awaitDataFrames(2);
                    Assert.assertEquals(0, frames.get(0)[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
                    Assert.assertEquals(0, frames.get(0)[QwpConstants.HEADER_OFFSET_FLAGS]
                            & QwpConstants.FLAG_DEFER_COMMIT);
                    Assert.assertTrue((frames.get(1)[QwpConstants.HEADER_OFFSET_FLAGS]
                            & QwpConstants.FLAG_SCHEMA) != 0);
                    Assert.assertEquals(0, frames.get(1)[QwpConstants.HEADER_OFFSET_FLAGS]
                            & QwpConstants.FLAG_DEFER_COMMIT);
                    new FrameReader(frames.get(0)).legacyVarcharTable("events", "legacy_value", "A", "B", "C");
                    FrameReader schema = new FrameReader(frames.get(1));
                    schema.schemaTable("events", 61, 62, 1, "id", QwpConstants.TYPE_UUID);
                    Assert.assertEquals(0, schema.u8());
                    Assert.assertEquals(3, schema.i64());
                    Assert.assertEquals(4, schema.i64());
                    schema.eof();
                    Assert.assertEquals(1, schemaHandler.describeRequests.get());
                } finally {
                    sender.close();
                    legacy.close();
                    if (upgraded != null) {
                        upgraded.close();
                    }
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testAsyncWritesLegacyOfflineThenUsesSchemaAfterSupportingConnect() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";initial_connect_retry=async;reconnect_initial_backoff_millis=10;"
                    + "reconnect_max_backoff_millis=50;auto_flush_rows=2147483647;"
                    + "auto_flush_bytes=0;auto_flush_interval=2147483646;"
                    + "close_flush_timeout_millis=0;");
            TestWebSocketServer server = null;
            try {
                Instant value = Instant.ofEpochSecond(3, 456_789_123);
                // No server is listening: this effective write must nevertheless
                // complete on the caller thread using the existing legacy conversion.
                sender.table("events").timestampColumn("ts", value).atNow();

                SchemaHandler handler = new SchemaHandler(
                        QwpSchemaProtocol.RESULT_KNOWN, 71, 72, true, -1);
                server = new TestWebSocketServer(handler, false, null, port);
                server.setAdvertiseSchema(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                Assert.assertTrue("schema-capable connection was not installed", handler.awaitPong());

                // The pending batch is legacy, so this row stays legacy too. The
                // first row after the flush adopts schema mode.
                sender.table("events").timestampColumn("ts", value).atNow();
                sender.flush();
                sender.table("events").timestampColumn("ts", value).atNow();
                sender.flush();

                List<byte[]> frames = handler.awaitDataFrames(2);
                new FrameReader(frames.get(0)).legacyTimestampTable(
                        "events", "ts", 3_456_789L, 3_456_789L);
                new FrameReader(frames.get(1)).schemaTimestampTable(
                        "events", 71, 72, "ts", 3_456_789_123L);
                Assert.assertEquals(1, handler.describeRequests.get());
            } finally {
                sender.close();
                if (server != null) {
                    server.close();
                }
            }
        });
    }

    @Test
    public void testSchemaBatchCapSplitsWithoutDroppingPinnedFlags() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 81, 82);
            TestWebSocketServer server = new TestWebSocketServer(handler);
            server.setAdvertiseSchema(true);
            server.setAdvertisedMaxBatchSize(64);
            try (TestWebSocketServer ignored = start(server);
                 Sender sender = sender(server)) {
                sender.table("a").uuidColumn("id", 1, 2).atNow();
                sender.table("b").uuidColumn("id", 3, 4).atNow();
                sender.flush();
                List<byte[]> frames = handler.awaitDataFrames(2);
                for (int i = 0; i < 2; i++) {
                    byte[] frame = frames.get(i);
                    Assert.assertTrue(frame.length <= 64);
                    Assert.assertTrue((frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA) != 0);
                    Assert.assertEquals(1,
                            ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN).getShort(6) & 0xffff);
                }
                Assert.assertTrue((frames.get(0)[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_DEFER_COMMIT) != 0);
                Assert.assertEquals(0, frames.get(1)[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_DEFER_COMMIT);
                FrameReader first = new FrameReader(frames.get(0));
                first.schemaTable("a", 81, 82, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, first.u8());
                Assert.assertEquals(1, first.i64());
                Assert.assertEquals(2, first.i64());
                first.eof();
                FrameReader second = new FrameReader(frames.get(1));
                second.schemaTable("b", 82, 82, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, second.u8());
                Assert.assertEquals(3, second.i64());
                Assert.assertEquals(4, second.i64());
                second.eof();
            }
        });
    }

    @Test(timeout = 10_000)
    public void testStrictSchemaLookupDeadlineFailsTypedThenRetryUsesAvailableSchema() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 91, 92);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";schema_mode=strict;schema_wait_millis=1000;"
                         + "initial_connect_retry=async;reconnect_initial_backoff_millis=10;"
                         + "reconnect_max_backoff_millis=50;auto_flush_rows=2147483647;"
                         + "auto_flush_bytes=0;auto_flush_interval=2147483646;"
                         + "close_flush_timeout_millis=0;")) {
                Assert.assertTrue("schema-capable connection was not installed", handler.awaitPong());
                QwpTableBuffer b = ((QwpWebSocketSender) sender).getTableBuffer("b");
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                QwpTableBuffer events = ((QwpWebSocketSender) sender).getTableBuffer("events");
                handler.respondToDescribe = false;
                try {
                    sender.table("b");
                    Assert.fail("expected schema negotiation deadline");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, e.getReason());
                }
                Assert.assertEquals(1, events.getRowCount());
                Assert.assertEquals(0, b.getRowCount());
                handler.respondToDescribe = true;
                sender.uuidColumn("id", 3, 4).atNow();
                Assert.assertEquals(1, events.getRowCount());
                Assert.assertEquals(1, b.getRowCount());
                sender.flush();
                Assert.assertTrue((handler.awaitDataFrame()[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_SCHEMA) != 0);
            }
        });
    }

    @Test
    public void testStrictInterruptedSchemaLookupFailsTypedAndPreservesInterrupt() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 93, 94);
            handler.respondToDescribe = false;
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";schema_mode=strict;initial_connect_retry=async;reconnect_initial_backoff_millis=10;"
                         + "reconnect_max_backoff_millis=50;close_flush_timeout_millis=0;")) {
                Assert.assertTrue("schema-capable connection was not installed", handler.awaitPong());
                ExecutorService executor = Executors.newSingleThreadExecutor();
                CountDownLatch entered = new CountDownLatch(1);
                AtomicReference<Thread> writer = new AtomicReference<>();
                AtomicBoolean interruptPreserved = new AtomicBoolean();
                Future<LineSenderSchemaException> result = executor.submit(() -> {
                    writer.set(Thread.currentThread());
                    entered.countDown();
                    try {
                        sender.table("events").uuidColumn("id", 1, 2);
                        return null;
                    } catch (LineSenderSchemaException e) {
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                        return e;
                    }
                });
                try {
                    Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
                    writer.get().interrupt();
                    LineSenderSchemaException error = result.get(5, TimeUnit.SECONDS);
                    Assert.assertNotNull(error);
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, error.getReason());
                    Assert.assertTrue("writer interrupt status was cleared", interruptPreserved.get());
                } finally {
                    executor.shutdownNow();
                    Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testStrictAsyncRecoveredSchemaBacklogWaitsForNegotiation() throws Exception {
        assertMemoryLeak(() -> {
            File sfRoot = temporaryFolder.newFolder("schema-required-recovery");
            int unavailablePort = publishUnackedSchemaBacklog(sfRoot, 95, 96);

            // STRICT never takes the legacy contract once schema support is known,
            // and the recovered backlog already requires it. With no server the
            // wait runs out its configured budget and fails typed.
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + unavailablePort
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=recovery;initial_connect_retry=async;schema_mode=strict;"
                    + "schema_wait_millis=250;"
                    + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;"
                    + "close_flush_timeout_millis=0;")) {
                try {
                    sender.table("events");
                    Assert.fail("recovered schema-required backlog fell back to legacy encoding");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, e.getReason());
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testAutoAsyncRecoveredSchemaBacklogWritesLegacyOfflineAndReplaysBacklog() throws Exception {
        assertMemoryLeak(() -> {
            File sfRoot = temporaryFolder.newFolder("schema-required-recovery-auto");
            int port = publishUnackedSchemaBacklog(sfRoot, 97, 98);

            TestWebSocketServer server = null;
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=recovery;initial_connect_retry=async;"
                    + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;"
                    + "auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                    + "close_flush_timeout_millis=0;")) {
                // AUTO (the default) never waits on a wire that has not come up. The
                // recovered backlog still pins the capability requirement for the
                // reconnect, but this row takes the legacy contract right away.
                sender.table("events").stringColumn("legacy_value", "A").atNow();

                SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 97, 98);
                server = new TestWebSocketServer(handler, false, null, port);
                server.setAdvertiseSchema(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                Assert.assertTrue("schema-capable connection was not installed", handler.awaitPong());
                sender.flush();

                // The recovered schema frame replays first, then the legacy batch.
                List<byte[]> frames = handler.awaitDataFrames(2);
                FrameReader recovered = new FrameReader(frames.get(0));
                recovered.schemaTable("events", 97, 98, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, recovered.u8());
                Assert.assertEquals(1, recovered.i64());
                Assert.assertEquals(2, recovered.i64());
                recovered.eof();
                new FrameReader(frames.get(1)).legacyVarcharTable("events", "legacy_value", "A");
                Assert.assertEquals(0, handler.describeRequests.get());
            } finally {
                if (server != null) {
                    server.close();
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testAutoOutageWritesUnseenTableLegacyAndNextBatchAdoptsSchema() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 111, 112);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "", disconnected);
            TestWebSocketServer restarted = null;
            try {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                sender.flush();
                Assert.assertTrue((first.awaitDataFrame()[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_SCHEMA) != 0);
                server.close();
                Assert.assertTrue("sender did not observe the connection closing",
                        disconnected.await(5, TimeUnit.SECONDS));

                // The wire is down and "other" is not cached. AUTO must not wait out
                // the 30 s default schema budget (the test timeout proves it); the row
                // takes the legacy contract and queues in store-and-forward.
                sender.table("other").stringColumn("legacy_value", "A").atNow();

                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 113, 114);
                restarted = schemaServer(second, port);
                Assert.assertTrue("schema-capable connection was not installed", second.awaitPong());
                sender.flush();
                // The first row after the flush consults the schema again: "other" is
                // looked up on the fresh connection and adopts the strict contract.
                sender.table("other").uuidColumn("id", 3, 4).atNow();
                sender.flush();

                List<byte[]> frames = second.awaitDataFrames(2);
                new FrameReader(frames.get(0)).legacyVarcharTable("other", "legacy_value", "A");
                FrameReader schema = new FrameReader(frames.get(1));
                schema.schemaTable("other", 113, 114, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, schema.u8());
                Assert.assertEquals(3, schema.i64());
                Assert.assertEquals(4, schema.i64());
                schema.eof();
                Assert.assertEquals(1, second.describeRequests.get());
            } finally {
                sender.close();
                server.close();
                if (restarted != null) {
                    restarted.close();
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testAutoOutagePublishesPendingSchemaBatchBeforeLegacyRow() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 121, 122);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "", disconnected);
            TestWebSocketServer restarted = null;
            try {
                // A schema row is pending and unflushed when the wire drops.
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                server.close();
                Assert.assertTrue("sender did not observe the connection closing",
                        disconnected.await(5, TimeUnit.SECONDS));

                // A QWP frame has one wire family: the legacy row cannot join the
                // pending schema batch, so that batch is published to store-and-forward
                // first and the legacy row opens a new batch.
                sender.table("other").stringColumn("legacy_value", "A").atNow();
                Assert.assertEquals(0, ((QwpWebSocketSender) sender).getTableBuffer("events").getRowCount());

                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 121, 122);
                restarted = schemaServer(second, port);
                Assert.assertTrue("schema-capable connection was not installed", second.awaitPong());
                sender.flush();

                List<byte[]> frames = second.awaitDataFrames(2);
                FrameReader schema = new FrameReader(frames.get(0));
                schema.schemaTable("events", 121, 122, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, schema.u8());
                Assert.assertEquals(1, schema.i64());
                Assert.assertEquals(2, schema.i64());
                schema.eof();
                new FrameReader(frames.get(1)).legacyVarcharTable("other", "legacy_value", "A");
                Assert.assertEquals(0, second.describeRequests.get());
            } finally {
                sender.close();
                server.close();
                if (restarted != null) {
                    restarted.close();
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testStrictOutageFailsUnseenTableWithinSchemaWaitAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 131, 132);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "schema_mode=strict;schema_wait_millis=250;", disconnected);
            TestWebSocketServer restarted = null;
            try {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                sender.flush();
                Assert.assertTrue((first.awaitDataFrame()[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_SCHEMA) != 0);
                server.close();
                Assert.assertTrue("sender did not observe the connection closing",
                        disconnected.await(5, TimeUnit.SECONDS));

                // STRICT keeps waiting for the wire and fails typed at the budget.
                try {
                    sender.table("other");
                    Assert.fail("STRICT fell back to legacy encoding during an outage");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, e.getReason());
                }
                // A cached table is still writable offline: "events" was pinned by the
                // flushed batch's snapshot and the cache survives until reconnect.
                sender.table("events").uuidColumn("id", 3, 4).atNow();

                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 131, 132);
                restarted = schemaServer(second, port);
                Assert.assertTrue("schema-capable connection was not installed", second.awaitPong());
                sender.table("other").uuidColumn("id", 5, 6).atNow();
                sender.flush();

                List<byte[]> frames = second.awaitDataFrames(1);
                Assert.assertTrue((frames.get(0)[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_SCHEMA) != 0);
                Assert.assertEquals(2, ByteBuffer.wrap(frames.get(0)).order(ByteOrder.LITTLE_ENDIAN)
                        .getShort(6) & 0xffff);
                Assert.assertEquals(1, second.describeRequests.get());
            } finally {
                sender.close();
                server.close();
                if (restarted != null) {
                    restarted.close();
                }
            }
        });
    }

    @Test(timeout = 15_000)
    public void testOffRetainedLegacyFramesFailOverFromCapableToLegacyPeer() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142, false);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "schema_mode=off;", disconnected);
            TestWebSocketServer replacement = null;
            try {
                sender.table("events").stringColumn("legacy_value", "A").atNow();
                sender.flush();
                byte[] retained = first.awaitDataFrame();
                server.close();
                Assert.assertTrue(disconnected.await(5, TimeUnit.SECONDS));
                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
                replacement = start(new TestWebSocketServer(second, false, null, port));
                Assert.assertArrayEquals(retained, second.awaitDataFrame());
                Assert.assertTrue(sender.drain(5_000));
                Assert.assertFalse(server.hasRequestedSchema());
                Assert.assertFalse(replacement.hasRequestedSchema());
                Assert.assertEquals(0, first.describeRequests.get());
                Assert.assertEquals(0, second.describeRequests.get());
            } finally {
                sender.close();
                server.close();
                if (replacement != null) {
                    replacement.close();
                }
            }
        });
    }

    @Test(timeout = 15_000)
    public void testStrictRemembersSchemaHandshakeWithoutPublishedFrames() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "schema_mode=strict;schema_wait_millis=100;", disconnected);
            TestWebSocketServer replacement = null;
            try {
                Assert.assertTrue(first.awaitPong());
                server.close();
                Assert.assertTrue(disconnected.await(5, TimeUnit.SECONDS));
                LineSenderSchemaException outage = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.table("events"));
                Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, outage.getReason());
                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
                replacement = start(new TestWebSocketServer(second, false, null, port));
                Assert.assertTrue("legacy replacement was not installed", second.awaitPong());
                LineSenderSchemaException downgrade = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.table("other"));
                Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, downgrade.getReason());
                Assert.assertEquals(0, first.describeRequests.get());
                Assert.assertEquals(0, second.describeRequests.get());
                Assert.assertEquals(0, second.dataFrames.size());
            } finally {
                sender.close();
                server.close();
                if (replacement != null) {
                    replacement.close();
                }
            }
        });
    }

    @Test(timeout = 15_000)
    public void testAutoDowngradesBeforePublishingSchemaFrames() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            Sender sender = outageSender(port, "", disconnected);
            TestWebSocketServer replacement = null;
            try {
                // Lookup alone must not turn a schema-capable peer into a replay requirement.
                sender.table("events");
                Assert.assertEquals(1, first.describeRequests.get());
                server.close();
                Assert.assertTrue(disconnected.await(5, TimeUnit.SECONDS));
                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
                replacement = start(new TestWebSocketServer(second, false, null, port));
                Assert.assertTrue("legacy replacement was not installed", second.awaitPong());
                sender.table("other").stringColumn("legacy_value", "A").atNow();
                sender.flush();
                new FrameReader(second.awaitDataFrame()).legacyVarcharTable("other", "legacy_value", "A");
                Assert.assertEquals(0, second.describeRequests.get());
            } finally {
                sender.close();
                server.close();
                if (replacement != null) {
                    replacement.close();
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testSchemaModeOffNeverDescribesOnSchemaCapableServer() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 141, 142);
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";schema_mode=off;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                         + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                sender.table("events").stringColumn("legacy_value", "A").atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrame()).legacyVarcharTable("events", "legacy_value", "A");
                Assert.assertEquals(0, handler.describeRequests.get());
                Assert.assertFalse(server.hasRequestedSchema());
            }
        });
    }

    @Test(timeout = 10_000)
    public void testAutoAbandonsLookupWhenWireDropsUnderIt() throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            SchemaHandler first = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 151, 152);
            first.respondToDescribe = false;
            TestWebSocketServer server = schemaServer(first, port);
            CountDownLatch disconnected = new CountDownLatch(1);
            // A budget far beyond the test timeout: only the wire drop can end the wait.
            Sender sender = outageSender(port, "schema_wait_millis=60000;", disconnected);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            TestWebSocketServer restarted = null;
            try {
                Future<?> row = executor.submit(() -> {
                    sender.table("events").stringColumn("legacy_value", "A").atNow();
                    return null;
                });
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (first.describeRequests.get() == 0) {
                    Assert.assertTrue("lookup was never sent", System.nanoTime() < deadline);
                    Thread.sleep(5);
                }
                server.close();
                Assert.assertTrue("sender did not observe the connection closing",
                        disconnected.await(5, TimeUnit.SECONDS));
                // The connection loss fails the pending lookup and AUTO falls back to
                // the legacy contract instead of sitting out the schema budget.
                row.get(5, TimeUnit.SECONDS);

                SchemaHandler second = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 151, 152);
                restarted = schemaServer(second, port);
                Assert.assertTrue("schema-capable connection was not installed", second.awaitPong());
                sender.flush();
                new FrameReader(second.awaitDataFrame()).legacyVarcharTable("events", "legacy_value", "A");
            } finally {
                executor.shutdownNow();
                sender.close();
                server.close();
                if (restarted != null) {
                    restarted.close();
                }
            }
        });
    }

    @Test
    public void testResetDiscardsRowsAndKeepsCachedSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, 101, 102);
            handler.isSchemaFeedbackEnabled = true;
            try (TestWebSocketServer server = schemaServer(handler);
                 Sender sender = sender(server)) {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                handler.version = 103;
                sender.table("events");
                try {
                    sender.stringColumn("id", "invalid");
                    Assert.fail("expected UUID validation error");
                } catch (LineSenderSchemaException e) {
                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                }
                // reset() discards the rows without sending anything, so no ACK can
                // deliver version 103: the next batch still pins the cached 102.
                sender.reset();
                sender.table("events").uuidColumn("id", 3, 4).atNow();
                long fsn = sender.flushAndGetSequence();
                FrameReader reader = new FrameReader(handler.awaitDataFrame());
                reader.schemaTable("events", 101, 102, 1, "id", QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(3, reader.i64());
                Assert.assertEquals(4, reader.i64());
                reader.eof();
                // That frame's ACK carries 103 for the batch after it.
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 5_000));
                sender.table("events").uuidColumn("id", 5, 6).atNow();
                sender.flush();
                new FrameReader(handler.awaitDataFrames(2).get(1)).uuidTable("events", 101, 103, 5, 6);
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    @Test
    public void testNormalSenderAutomaticallyNegotiatesBeforeFirstEffectiveValue() throws Exception {
        assertMemoryLeak(() -> {
            DescribeUuidHandler handler = new DescribeUuidHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertiseSchema(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";close_flush_timeout_millis=0;")) {
                    sender.table("events");
                    try {
                        sender.stringColumn("id", "not-a-uuid");
                        sender.cancelRow();
                        Assert.fail("expected schema-directed UUID validation");
                    } catch (LineSenderSchemaException e) {
                        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                    }
                }
                Assert.assertEquals(1, handler.describeRequests.get());
            }
        });
    }

    private static final class DescribeUuidHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger describeRequests = new AtomicInteger();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ByteBuffer input = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            if (input.getInt(0) != QwpConstants.MAGIC_MESSAGE
                    || data[5] != QwpSchemaProtocol.FLAG_CONTROL) {
                return;
            }
            describeRequests.incrementAndGet();
            long requestId = input.getLong(QwpConstants.HEADER_SIZE + 1);
            byte[] column = "id".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1 + 4 + 8 + 2 + 2 + 2 + column.length + 4 + 2;
            ByteBuffer response = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLength)
                    .order(ByteOrder.LITTLE_ENDIAN);
            response.putInt(QwpConstants.MAGIC_MESSAGE)
                    .put((byte) QwpConstants.VERSION)
                    .put(QwpSchemaProtocol.FLAG_CONTROL)
                    .putShort((short) 0)
                    .putInt(payloadLength)
                    .put(QwpSchemaProtocol.KIND_SCHEMA)
                    .putLong(requestId)
                    .put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                    .putInt(17)
                    .putLong(23)
                    .putShort((short) -1)
                    .putShort((short) 1)
                    .putShort((short) column.length)
                    .put(column)
                    .putInt(ColumnType.UUID)
                    .putShort((short) 0);
            try {
                client.sendBinary(response.array());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static TestWebSocketServer legacyServer(SchemaHandler handler) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        try {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            return server;
        } catch (Throwable t) {
            server.close();
            throw t;
        }
    }

    /**
     * A sender for the outage tests: no auto-flush, fast reconnect backoff, and a
     * connection listener that trips {@code disconnected} when the wire drops.
     * {@code extraConfig} is appended verbatim (e.g. {@code schema_mode=strict;}).
     */
    private static Sender outageSender(int port, String extraConfig, CountDownLatch disconnected) {
        return Sender.builder("ws::addr=localhost:" + port
                        + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;"
                        + "close_flush_timeout_millis=0;" + extraConfig)
                .connectionListener(event -> {
                    if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                        disconnected.countDown();
                    }
                })
                .build();
    }

    /**
     * Writes one schema-encoded UUID row for "events" through a sender rooted at
     * {@code sfRoot} against a server that never acknowledges data, so the frame
     * stays in store-and-forward for a later sender to recover. Returns the port
     * the server listened on, which is free again when this method returns.
     */
    private static int publishUnackedSchemaBacklog(File sfRoot, int tableId, long version) throws Exception {
        SchemaHandler handler = new SchemaHandler(QwpSchemaProtocol.RESULT_KNOWN, tableId, version, false);
        try (TestWebSocketServer server = schemaServer(handler)) {
            String cfg = "ws::addr=localhost:" + server.getPort()
                    + ";sf_dir=" + sfRoot.getAbsolutePath()
                    + ";sender_id=recovery;close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("events").uuidColumn("id", 1, 2).atNow();
                sender.flush();
                Assert.assertTrue((handler.awaitDataFrame()[QwpConstants.HEADER_OFFSET_FLAGS]
                        & QwpConstants.FLAG_SCHEMA) != 0);
            }
            return server.getPort();
        }
    }

    private static TestWebSocketServer schemaServer(TestWebSocketServer.WebSocketServerHandler handler) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.setAdvertiseSchema(true);
        return start(server);
    }

    private static TestWebSocketServer schemaServer(TestWebSocketServer.WebSocketServerHandler handler, int port) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler, false, null, port);
        server.setAdvertiseSchema(true);
        return start(server);
    }

    private static TestWebSocketServer start(TestWebSocketServer server) throws Exception {
        try {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            return server;
        } catch (Throwable t) {
            server.close();
            throw t;
        }
    }

    private static byte numericWireType(int targetType) {
        switch (targetType) {
            case ColumnType.BYTE:
                return QwpConstants.TYPE_BYTE;
            case ColumnType.SHORT:
                return QwpConstants.TYPE_SHORT;
            case ColumnType.INT:
                return QwpConstants.TYPE_INT;
            case ColumnType.LONG:
                return QwpConstants.TYPE_LONG;
            case ColumnType.FLOAT:
                return QwpConstants.TYPE_FLOAT;
            case ColumnType.DOUBLE:
                return QwpConstants.TYPE_DOUBLE;
            default:
                throw new AssertionError(ColumnType.nameOf(targetType));
        }
    }

    private static byte temporalWireType(int targetType) {
        switch (targetType) {
            case ColumnType.DATE:
                return QwpConstants.TYPE_DATE;
            case ColumnType.TIMESTAMP_MICRO:
                return QwpConstants.TYPE_TIMESTAMP;
            case ColumnType.TIMESTAMP_NANO:
                return QwpConstants.TYPE_TIMESTAMP_NANOS;
            default:
                throw new AssertionError(ColumnType.nameOf(targetType));
        }
    }

    private static Sender sender(TestWebSocketServer server) {
        return Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                + "close_flush_timeout_millis=0;");
    }

    private static final class DecimalTextVector {
        private final String caseId;
        private final String expectedText;
        private final String inputForm;
        private final String sourceType;
        private final int sourceScale;
        private final String[] limbs;
        private final String targetType;

        private DecimalTextVector(String[] fields) {
            Assert.assertEquals(String.join("\t", fields), 10, fields.length);
            caseId = fields[0];
            sourceType = fields[1];
            sourceScale = Integer.parseInt(fields[2]);
            limbs = new String[]{fields[3], fields[4], fields[5], fields[6]};
            targetType = fields[7];
            inputForm = fields[8];
            expectedText = fields[9];
        }

        private void append(Sender sender, String column) {
            if ("JAVA_NULL".equals(inputForm)) {
                appendJavaNull(sender, column);
                return;
            }
            if ("NULL_VALUE".equals(inputForm)) {
                switch (sourceType) {
                    case "DECIMAL64":
                        sender.decimalColumn(column, Decimal64.NULL_VALUE);
                        return;
                    case "DECIMAL128":
                        sender.decimalColumn(column, Decimal128.NULL_VALUE);
                        return;
                    case "DECIMAL256":
                        sender.decimalColumn(column, Decimal256.NULL_VALUE);
                        return;
                    default:
                        throw new AssertionError(sourceType);
                }
            }
            Assert.assertEquals("VALUE", inputForm);
            switch (sourceType) {
                case "DECIMAL64":
                    sender.decimalColumn(column, new Decimal64(limb(0), sourceScale));
                    return;
                case "DECIMAL128":
                    sender.decimalColumn(column, new Decimal128(limb(1), limb(0), sourceScale));
                    return;
                case "DECIMAL256":
                    sender.decimalColumn(column,
                            new Decimal256(limb(3), limb(2), limb(1), limb(0), sourceScale));
                    return;
                default:
                    throw new AssertionError(sourceType);
            }
        }

        private void appendJavaNull(Sender sender, String column) {
            switch (sourceType) {
                case "DECIMAL64":
                    sender.decimalColumn(column, (Decimal64) null);
                    return;
                case "DECIMAL128":
                    sender.decimalColumn(column, (Decimal128) null);
                    return;
                case "DECIMAL256":
                    sender.decimalColumn(column, (Decimal256) null);
                    return;
                default:
                    throw new AssertionError(sourceType);
            }
        }

        private boolean isNull() {
            return !"VALUE".equals(inputForm);
        }

        private long limb(int index) {
            Assert.assertNotEquals("-", limbs[index]);
            return Long.parseUnsignedLong(limbs[index], 16);
        }

        private int sourceIndex() {
            switch (sourceType) {
                case "DECIMAL64":
                    return 0;
                case "DECIMAL128":
                    return 1;
                case "DECIMAL256":
                    return 2;
                default:
                    throw new AssertionError(sourceType);
            }
        }

        private int targetIndex() {
            if ("STRING".equals(targetType)) {
                return 0;
            }
            if ("VARCHAR".equals(targetType)) {
                return 1;
            }
            throw new AssertionError(targetType);
        }

        private int targetType() {
            if ("STRING".equals(targetType)) {
                return ColumnType.STRING;
            }
            if ("VARCHAR".equals(targetType)) {
                return ColumnType.VARCHAR;
            }
            throw new AssertionError(targetType);
        }
    }

    /**
     * Builds the cumulative ACK the fake servers send for a data frame. Like the real
     * server, a handler attaches the current schema of {@code tableName} when the
     * snapshot it last delivered to the client is stale, so the client learns about
     * a change on the batch boundary without a DESCRIBE of its own.
     */
    private static byte[] ackWithSchemaUpdate(long sequence, String tableName, byte[] schemaPayload) {
        byte[] name = tableName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer ack = ByteBuffer.allocate(1 + 8 + 2 + 2 + 2 + name.length + 4 + schemaPayload.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        ack.put((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES))
                .putLong(sequence).putShort((short) 0)
                .putShort((short) 1).putShort((short) name.length).put(name)
                .putInt(schemaPayload.length).put(schemaPayload);
        return ack.array();
    }

    /** The ACK a server sends when it cannot produce the schema snapshot a stale frame calls for. */
    private static byte[] ackInvalidateAll(long sequence) {
        ByteBuffer ack = ByteBuffer.allocate(1 + 8 + 2).order(ByteOrder.LITTLE_ENDIAN);
        ack.put((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL))
                .putLong(sequence).putShort((short) 0);
        return ack.array();
    }

    private static byte[] schemaControlFrame(byte[] schemaPayload) {
        ByteBuffer out = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + schemaPayload.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) QwpConstants.VERSION)
                .put(QwpSchemaProtocol.FLAG_CONTROL).putShort((short) 0).putInt(schemaPayload.length)
                .put(schemaPayload);
        return out.array();
    }

    /**
     * Walks one QWP data frame, or a sequence of single-table frames. A table
     * pins one schema snapshot per batch, so two schema generations of the same
     * table always arrive in two frames; the two-block helpers read the second
     * block from the second frame, skipping its message header and symbol
     * dictionary prelude.
     */
    private static final class FrameReader {
        private final List<byte[]> frames;
        private int frameIndex;
        private ByteBuffer in;

        private FrameReader(byte[] data) {
            this(Collections.singletonList(data));
        }

        private FrameReader(List<byte[]> frames) {
            this.frames = frames;
            this.in = ByteBuffer.wrap(frames.get(0)).order(ByteOrder.LITTLE_ENDIAN);
        }

        private void nextFrameIfExhausted() {
            if (in.hasRemaining() || frameIndex + 1 >= frames.size()) {
                return;
            }
            frameIndex++;
            in = ByteBuffer.wrap(frames.get(frameIndex)).order(ByteOrder.LITTLE_ENDIAN);
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertTrue((u8() & QwpConstants.FLAG_SCHEMA) != 0);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            varint();
            for (int i = 0, n = varint(); i < n; i++) {
                string();
            }
        }

        private long i64() {
            return in.getLong();
        }

        private void legacyVarcharTable(String table, String column, String... values) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(values.length, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            int total = 0;
            StringBuilder joined = new StringBuilder();
            for (String value : values) {
                joined.append(value);
                total += value.getBytes(StandardCharsets.UTF_8).length;
                Assert.assertEquals(total, in.getInt());
            }
            byte[] bytes = joined.toString().getBytes(StandardCharsets.UTF_8);
            byte[] actual = new byte[total];
            in.get(actual);
            Assert.assertArrayEquals(bytes, actual);
            eof();
        }

        private void legacyTimestampTable(String table, String column, long... expectedMicros) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(expectedMicros.length, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            for (long micros : expectedMicros) {
                Assert.assertEquals(micros, i64());
            }
            eof();
        }

        private void schemaTimestampTable(
                String table,
                int tableId,
                long version,
                String column,
                long expectedNanos
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 1, column, QwpConstants.TYPE_TIMESTAMP_NANOS);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(expectedNanos, i64());
            eof();
        }

        private void legacyIpv4Table(String table, String column, int... values) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(values.length, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(QwpConstants.TYPE_IPv4, u8());
            Assert.assertEquals(0, u8());
            for (int value : values) {
                Assert.assertEquals(value, in.getInt());
            }
            eof();
        }

        private void legacyLong256Table(String table, String column) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(2, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(QwpConstants.TYPE_LONG256, u8());
            Assert.assertEquals(0, u8());
            for (long value : new long[]{1, 2, 3, 4}) {
                Assert.assertEquals(value, i64());
            }
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(Long.MIN_VALUE, i64());
            }
            eof();
        }

        private void legacySmallIntegerTable(String table) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(3, varint());
            Assert.assertEquals("b", string());
            Assert.assertEquals(QwpConstants.TYPE_BYTE, u8());
            Assert.assertEquals("s", string());
            Assert.assertEquals(QwpConstants.TYPE_SHORT, u8());
            Assert.assertEquals("i", string());
            Assert.assertEquals(QwpConstants.TYPE_INT, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals((byte) -7 & 0xff, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(42, in.getShort());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(Integer.MIN_VALUE, in.getInt());
            eof();
        }

        private void longTextTable(
                String table,
                int tableId,
                long version,
                int targetType,
                String expectedValue
        ) {
            messageHeader(1);
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(2, varint());
                // Global dictionary registration is append-only: the rolled-back B value
                // remains an unused dictionary entry, while no B row survives.
                Assert.assertEquals("7", string());
                Assert.assertEquals(expectedValue, string());
            } else {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(0, varint());
            }
            schemaBlockHeader(table, tableId, version, 2, "value",
                    targetType == ColumnType.SYMBOL ? QwpConstants.TYPE_SYMBOL : QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(1, varint());
            } else {
                Assert.assertEquals(0, in.getInt());
                byte[] expected = expectedValue.getBytes(StandardCharsets.UTF_8);
                Assert.assertEquals(expected.length, in.getInt());
                byte[] actual = new byte[expected.length];
                in.get(actual);
                Assert.assertArrayEquals(expected, actual);
            }
            eof();
        }

        private void ipv4Table(String table, int tableId, long version, int targetType, int expectedValue) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            byte wireType = targetType == ColumnType.IPv4
                    ? QwpConstants.TYPE_IPv4
                    : QwpConstants.TYPE_VARCHAR;
            schemaBlockHeader(table, tableId, version, 2, "value", wireType);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            if (wireType == QwpConstants.TYPE_IPv4) {
                Assert.assertEquals(expectedValue, in.getInt());
            } else {
                Assert.assertEquals(0, in.getInt());
                Assert.assertEquals(9, in.getInt());
                Assert.assertEquals("255.1.2.3", stringBytes(9));
            }
            eof();
        }

        private void intToIpv4Table(String table, int tableId, long version) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 4, "value", QwpConstants.TYPE_IPv4);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(3, u8());
            Assert.assertEquals(1, in.getInt());
            Assert.assertEquals(-1, in.getInt());
            eof();
        }

        private void long256TargetTable(
                String table,
                int tableId,
                long version,
                int targetType,
                long l0,
                long l1,
                long l2,
                long l3
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            byte wireType = targetType == ColumnType.LONG256
                    ? QwpConstants.TYPE_LONG256
                    : QwpConstants.TYPE_VARCHAR;
            schemaBlockHeader(table, tableId, version, 2, "value", wireType);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            if (wireType == QwpConstants.TYPE_LONG256) {
                Assert.assertEquals(l0, i64());
                Assert.assertEquals(l1, i64());
                Assert.assertEquals(l2, i64());
                Assert.assertEquals(l3, i64());
            } else {
                String expected = "0x04000000000000000300000000000000020000000000000001";
                Assert.assertEquals(0, in.getInt());
                Assert.assertEquals(expected.length(), in.getInt());
                Assert.assertEquals(expected, stringBytes(expected.length()));
            }
            eof();
        }

        private void long256Table(String table, int tableId, long version) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_LONG256);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0xfedcba9876543210L, i64());
            Assert.assertEquals(0x0123456789abcdefL, i64());
            Assert.assertEquals(0, i64());
            Assert.assertEquals(0, i64());
            eof();
        }

        private void geoHashTable(
                String table, int tableId, long version, int bits, int first, int second
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_GEOHASH);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(bits, varint());
            Assert.assertEquals(first, u8());
            Assert.assertEquals(second, u8());
            eof();
        }

        private void geoHashTargetTable(
                String table,
                int tableId,
                long version,
                int targetType,
                long first,
                long second,
                int precision
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            byte wireType = ColumnType.isGeoHash(targetType)
                    ? QwpConstants.TYPE_GEOHASH
                    : QwpConstants.TYPE_VARCHAR;
            schemaBlockHeader(table, tableId, version, 2, "value", wireType);
            if (wireType == QwpConstants.TYPE_GEOHASH) {
                Assert.assertEquals(1, u8());
                Assert.assertEquals(0, u8());
                Assert.assertEquals(precision, varint());
                for (long value : new long[]{first, second}) {
                    for (int i = 0; i < (precision + 7) / 8; i++) {
                        Assert.assertEquals((int) ((value >>> (i * 8)) & 0xff), u8());
                    }
                }
            } else {
                String firstText = "11010000110001101100";
                String secondText = "00010010001101000101";
                Assert.assertEquals(0, u8());
                Assert.assertEquals(0, in.getInt());
                Assert.assertEquals(firstText.length(), in.getInt());
                Assert.assertEquals(firstText.length() + secondText.length(), in.getInt());
                Assert.assertEquals(firstText, stringBytes(firstText.length()));
                Assert.assertEquals(secondText, stringBytes(secondText.length()));
            }
            eof();
        }

        private void legacyGeoHashTable(String table, String column) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertEquals(0, u8() & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            Assert.assertEquals(2, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(QwpConstants.TYPE_GEOHASH, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(5, varint());
            Assert.assertEquals(3, u8());
            Assert.assertEquals(26, u8());
            eof();
        }

        private void decimal64Table(
                String table, int tableId, long version, int scale, long first, long second
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(scale, u8());
            Assert.assertEquals(first, i64());
            Assert.assertEquals(second, i64());
            eof();
        }

        private void decimalTextLifecycleTable(String table, int tableId, long version) {
            StringBuilder maxScale = new StringBuilder(78).append("0.");
            for (int i = 0; i < 76; i++) {
                maxScale.append('9');
            }
            String expected = "123.40-0.125" + maxScale + '0';

            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 5, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0x04, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(6, in.getInt());
            Assert.assertEquals(12, in.getInt());
            Assert.assertEquals(90, in.getInt());
            Assert.assertEquals(91, in.getInt());
            Assert.assertEquals(expected, stringBytes(expected.length()));
            eof();
        }

        private void decimalTextTable(String table, int tableId, long version, String... expectedValues) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, expectedValues.length, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            int totalLength = 0;
            StringBuilder expected = new StringBuilder();
            for (String value : expectedValues) {
                expected.append(value);
                totalLength += value.getBytes(StandardCharsets.UTF_8).length;
                Assert.assertEquals(totalLength, in.getInt());
            }
            byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);
            byte[] actual = new byte[totalLength];
            in.get(actual);
            Assert.assertArrayEquals(expectedBytes, actual);
            eof();
        }

        private void parsedDecimalTextTable(String table, int tableId, long version) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 4, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0x0c, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(6, in.getInt());
            Assert.assertEquals(12, in.getInt());
            Assert.assertEquals("123.451.2300", stringBytes(12));
            eof();
        }

        private void twoDecimalBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_DECIMAL128);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1_000, i64());
            Assert.assertEquals(0, i64());
            eof();
        }

        private void twoDecimal64ScaleBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(4, u8());
            Assert.assertEquals(12_345, i64());
            eof();
        }

        private void twoDecimal64AndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(4, u8());
            Assert.assertEquals(12_340, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(7, in.getInt());
            Assert.assertEquals("25.0000", stringBytes(7));
            eof();
        }

        private void twoDecimalWidthAndScaleBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(4, u8());
            Assert.assertEquals(10_000, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_DECIMAL128);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(5, u8());
            Assert.assertEquals(-8_446_744_073_709_551_616L, i64());
            Assert.assertEquals(0, i64());
            eof();
        }

        private void nullGeoHashTable(String table, int tableId, long version, int bits) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 1, "value", QwpConstants.TYPE_GEOHASH);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            Assert.assertEquals(bits, varint());
            eof();
        }

        private void smallIntegerNumericTable(String table, int tableId, long version, byte wireType) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 4, "value", wireType);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(4, u8());
            assertSmallIntegerValue(wireType, -1);
            assertSmallIntegerValue(wireType, 2);
            assertSmallIntegerValue(wireType, 3);
            eof();
        }

        private void smallIntegerDecimalTable(String table, int tableId, long version) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 4, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(4, u8());
            Assert.assertEquals(2, u8());
            Assert.assertEquals(4_200, i64());
            Assert.assertEquals(-700, i64());
            Assert.assertEquals(214_748_364_700L, i64());
            eof();
        }

        private void smallIntegerTextTable(String table, int tableId, long version, int targetType) {
            messageHeader(1);
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(4, varint());
                Assert.assertEquals("-128", string());
                Assert.assertEquals("99", string());
                Assert.assertEquals("32767", string());
                Assert.assertEquals("2147483647", string());
            } else {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(0, varint());
            }
            schemaBlockHeader(table, tableId, version, 4, "value",
                    targetType == ColumnType.SYMBOL ? QwpConstants.TYPE_SYMBOL : QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(4, u8());
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(2, varint());
                Assert.assertEquals(3, varint());
            } else {
                Assert.assertEquals(0, in.getInt());
                Assert.assertEquals(4, in.getInt());
                Assert.assertEquals(9, in.getInt());
                Assert.assertEquals(19, in.getInt());
                Assert.assertEquals("-128327672147483647", stringBytes(19));
            }
            eof();
        }

        private void integerTemporalTable(String table, int tableId, long version, byte wireType) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 5, "value", wireType);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(4, u8());
            if (wireType != QwpConstants.TYPE_DATE) {
                Assert.assertEquals(0, u8());
            }
            Assert.assertEquals(-1, i64());
            Assert.assertEquals(2, i64());
            Assert.assertEquals(3, i64());
            Assert.assertEquals(Long.MAX_VALUE, i64());
            eof();
        }

        private void assertSmallIntegerValue(byte wireType, long value) {
            switch (wireType) {
                case QwpConstants.TYPE_BYTE:
                    Assert.assertEquals((byte) value & 0xff, u8());
                    break;
                case QwpConstants.TYPE_SHORT:
                    Assert.assertEquals((short) value, in.getShort());
                    break;
                case QwpConstants.TYPE_INT:
                    Assert.assertEquals((int) value, in.getInt());
                    break;
                case QwpConstants.TYPE_LONG:
                    Assert.assertEquals(value, in.getLong());
                    break;
                case QwpConstants.TYPE_FLOAT:
                    Assert.assertEquals(Float.floatToRawIntBits((float) value), in.getInt());
                    break;
                case QwpConstants.TYPE_DOUBLE:
                    Assert.assertEquals(Double.doubleToRawLongBits((double) value), in.getLong());
                    break;
                default:
                    throw new AssertionError(wireType);
            }
        }

        private void twoGeoHashBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_GEOHASH);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(60, varint());
            Assert.assertEquals(0x443214c74254bL, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_GEOHASH);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(5, varint());
            Assert.assertEquals(31, u8());
            eof();
        }

        private void twoDateAndTimestampBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DATE);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(11, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_TIMESTAMP_NANOS);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(12, i64());
            eof();
        }

        private void twoLong256AndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_LONG256);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0xfedcba9876543210L, i64());
            Assert.assertEquals(0x0123456789abcdefL, i64());
            Assert.assertEquals(0, i64());
            Assert.assertEquals(0, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(11, in.getInt());
            Assert.assertEquals("not-long256", stringBytes(11));
            eof();
        }

        private void twoNativeLong256AndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_LONG256);
            Assert.assertEquals(0, u8());
            for (long value : new long[]{1, 2, 3, 4}) {
                Assert.assertEquals(value, i64());
            }
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            String expected = "0x08000000000000000700000000000000060000000000000005";
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(expected.length(), in.getInt());
            Assert.assertEquals(expected, stringBytes(expected.length()));
            eof();
        }

        private void twoNativeGeoHashAndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_GEOHASH);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(20, varint());
            Assert.assertEquals(0xde, u8());
            Assert.assertEquals(0xbc, u8());
            Assert.assertEquals(0x0a, u8());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            String expected = "00010010001101000101";
            Assert.assertEquals(expected.length(), in.getInt());
            Assert.assertEquals(expected, stringBytes(expected.length()));
            eof();
        }

        private void longTimestampTable(
                String table, int tableId, long version, int targetType, long expectedValue
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value",
                    targetType == ColumnType.TIMESTAMP_MICRO
                            ? QwpConstants.TYPE_TIMESTAMP
                            : QwpConstants.TYPE_TIMESTAMP_NANOS);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(expectedValue, i64());
            eof();
        }

        private void twoLongAndTimestampBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_LONG);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(11, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_TIMESTAMP_NANOS);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(12, i64());
            eof();
        }

        private void twoVarcharAndTimestampBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(8, in.getInt());
            Assert.assertEquals("native-a", stringBytes(8));
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_TIMESTAMP);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, i64());
            eof();
        }

        private void timestampTextTable(
                String table, int tableId, long version, String expectedLastValue
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(0, in.getInt());
            byte[] expected = expectedLastValue.getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals(expected.length, in.getInt());
            byte[] actual = new byte[expected.length];
            in.get(actual);
            Assert.assertArrayEquals(expected, actual);
            eof();
        }

        private void stringDateTable(
                String table, int tableId, long version, long expectedValue
        ) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_DATE);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            Assert.assertEquals(expectedValue, i64());
            eof();
        }

        private void doubleArrayTable(String table, int tableId, long version) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_DOUBLE_ARRAY);
            Assert.assertEquals(0, u8());
            assertDoubleArray(new int[]{1, 2}, 1.0, 2.0);
            assertDoubleArray(new int[]{2, 1}, 3.0, 4.0);
            eof();
        }

        private void twoDoubleArrayRankBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DOUBLE_ARRAY);
            Assert.assertEquals(0, u8());
            assertDoubleArray(new int[]{1, 2}, 1.0, 2.0);
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_DOUBLE_ARRAY);
            Assert.assertEquals(0, u8());
            assertDoubleArray(new int[]{2}, 3.0, 4.0);
            eof();
        }

        private void assertDoubleArray(int[] shape, double... values) {
            Assert.assertEquals(shape.length, u8());
            for (int dimension : shape) {
                Assert.assertEquals(dimension, in.getInt());
            }
            for (double value : values) {
                Assert.assertEquals(Double.doubleToRawLongBits(value), in.getLong());
            }
        }

        private void twoDateAndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DATE);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(8, in.getInt());
            Assert.assertEquals("native-a", stringBytes(8));
            eof();
        }

        private void charTable(String table, int tableId, long version, char first, char second) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, 2, "value", QwpConstants.TYPE_CHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(first, in.getChar());
            Assert.assertEquals(second, in.getChar());
            eof();
        }

        private void twoVarcharAndCharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(8, in.getInt());
            Assert.assertEquals("native-a", stringBytes(8));
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_CHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0xffff, in.getChar());
            eof();
        }

        private void twoTimestampAndVarcharBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_TIMESTAMP_NANOS);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(Long.MIN_VALUE, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            String expected = "2262-04-11T23:47:16.854Z";
            byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals(bytes.length, in.getInt());
            byte[] actual = new byte[bytes.length];
            in.get(actual);
            Assert.assertArrayEquals(bytes, actual);
            eof();
        }

        private void floatingTextTable(
                String table,
                int tableId,
                long version,
                int targetType,
                String rolledBackValue,
                String expectedValue
        ) {
            messageHeader(1);
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(2, varint());
                Assert.assertEquals(rolledBackValue, string());
                Assert.assertEquals(expectedValue, string());
            } else {
                Assert.assertEquals(0, varint());
                Assert.assertEquals(0, varint());
            }
            schemaBlockHeader(table, tableId, version, 2, "value",
                    targetType == ColumnType.SYMBOL ? QwpConstants.TYPE_SYMBOL : QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(1, u8());
            Assert.assertEquals(1, u8());
            if (targetType == ColumnType.SYMBOL) {
                Assert.assertEquals(1, varint());
            } else {
                Assert.assertEquals(0, in.getInt());
                byte[] expected = expectedValue.getBytes(StandardCharsets.UTF_8);
                Assert.assertEquals(expected.length, in.getInt());
                byte[] actual = new byte[expected.length];
                in.get(actual);
                Assert.assertArrayEquals(expected, actual);
            }
            eof();
        }

        private void messageHeader(int tableCount) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertTrue((u8() & QwpConstants.FLAG_SCHEMA) != 0);
            if (frames.size() == 1) {
                Assert.assertEquals(tableCount, in.getShort() & 0xffff);
            } else {
                // One generation per frame: the blocks span the frame sequence.
                Assert.assertEquals(tableCount, frames.size());
                Assert.assertEquals(1, in.getShort() & 0xffff);
            }
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
        }

        private void schemaBlockHeader(
                String table,
                int tableId,
                long version,
                int rows,
                String column,
                byte wireType
        ) {
            nextFrameIfExhausted();
            Assert.assertEquals(table, string());
            Assert.assertEquals(1, u8());
            Assert.assertEquals(tableId, in.getInt());
            Assert.assertEquals(version, in.getLong());
            Assert.assertEquals(rows, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(column, string());
            Assert.assertEquals(wireType & 0xff, u8());
        }

        private void twoSymbolAndVarcharBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals("7", string());

            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_SYMBOL);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, varint());

            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(1, in.getInt());
            Assert.assertEquals('8', u8());
            eof();
        }

        private void twoSmallIntegerDecimalBlocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_DECIMAL128);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(5, u8());
            Assert.assertEquals(100_000_000, i64());
            Assert.assertEquals(0, i64());
            eof();
        }

        private void twoIntDecimalAndIpv4Blocks(
                String table, int tableId, long firstVersion, long secondVersion
        ) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DECIMAL64);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_IPv4);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1_000, in.getInt());
            eof();
        }

        private void twoUuidAndVarcharBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());

            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_UUID);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0x0102030405060708L, i64());
            Assert.assertEquals(0x1112131415161718L, i64());

            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(36, in.getInt());
            Assert.assertEquals("31323334-3536-3738-2122-232425262728", stringBytes(36));
            eof();
        }

        private void twoIpv4AndVarcharBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());

            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_IPv4);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0x01020304, in.getInt());

            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(7, in.getInt());
            Assert.assertEquals("5.6.7.8", stringBytes(7));
            eof();
        }

        private void twoDoubleAndVarcharBlocks(String table, int tableId, long firstVersion, long secondVersion) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, firstVersion, 1, "value", QwpConstants.TYPE_DOUBLE);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(Double.doubleToRawLongBits(-0.0), i64());
            schemaBlockHeader(table, tableId, secondVersion, 1, "value", QwpConstants.TYPE_VARCHAR);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0, in.getInt());
            Assert.assertEquals(6, in.getInt());
            Assert.assertEquals("1.0E23", stringBytes(6));
            eof();
        }

        private String stringBytes(int length) {
            byte[] bytes = new byte[length];
            in.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private void eof() {
            Assert.assertEquals("unread frames", frames.size() - 1, frameIndex);
            Assert.assertFalse("unexpected trailing column or value bytes", in.hasRemaining());
        }

        private String string() {
            int length = varint();
            byte[] bytes = new byte[length];
            in.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private void schemaTable(String table, int tableId, long version, int rows, String column, byte type) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, in.getInt());
            Assert.assertEquals(QwpConstants.VERSION, u8());
            Assert.assertTrue((u8() & QwpConstants.FLAG_SCHEMA) != 0);
            Assert.assertEquals(1, in.getShort() & 0xffff);
            Assert.assertEquals(in.remaining() - Integer.BYTES, in.getInt());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            Assert.assertEquals(table, string());
            if (tableId < 0) {
                Assert.assertEquals(0, u8());
            } else {
                Assert.assertEquals(1, u8());
                Assert.assertEquals(tableId, in.getInt());
                Assert.assertEquals(version, in.getLong());
            }
            Assert.assertEquals(rows, varint());
            int columns = varint();
            Assert.assertEquals(1, columns);
            Assert.assertEquals(column, string());
            Assert.assertEquals(type & 0xff, u8());
        }

        private void uuidTable(String table, int tableId, long version, long... loHiPairs) {
            messageHeader(1);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());
            schemaBlockHeader(table, tableId, version, loHiPairs.length / 2, "id", QwpConstants.TYPE_UUID);
            Assert.assertEquals(0, u8());
            for (long word : loHiPairs) {
                Assert.assertEquals(word, i64());
            }
            eof();
        }

        private void twoInferredAdoptionBlocks(String table) {
            messageHeader(2);
            Assert.assertEquals(0, varint());
            Assert.assertEquals(0, varint());

            Assert.assertEquals(table, string());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(1, varint());
            Assert.assertEquals(1, varint());
            Assert.assertEquals("id", string());
            Assert.assertEquals(QwpConstants.TYPE_LONG, u8());
            Assert.assertEquals(0, u8());
            Assert.assertEquals(11, i64());

            schemaBlockHeader(table, 31, 41, 1, "id", QwpConstants.TYPE_UUID);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(0x3132333435363738L, i64());
            Assert.assertEquals(0x2122232425262728L, i64());
            eof();
        }

        private void uuidOnlyTable(String table, int tableId, long version, long lo, long hi) {
            schemaTable(table, tableId, version, 1, "failed_b", QwpConstants.TYPE_UUID);
            Assert.assertEquals(0, u8());
            Assert.assertEquals(lo, i64());
            Assert.assertEquals(hi, i64());
            eof();
        }

        private int u8() {
            return in.get() & 0xff;
        }

        private int varint() {
            int result = 0;
            int shift = 0;
            int b;
            do {
                b = u8();
                result |= (b & 0x7f) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return result;
        }
    }

    private static final class SchemaHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final boolean acknowledgeData;
        private final List<byte[]> dataFrames = new ArrayList<>();
        private final CountDownLatch pong = new CountDownLatch(1);
        private final AtomicInteger describeRequests = new AtomicInteger();
        private final int designatedIndex;
        private volatile int result;
        private volatile boolean respondToDescribe = true;
        private final int tableId;
        private volatile long version;
        private long ackSequence;
        // The snapshot the client last received for "events", through DESCRIBE or ACK
        // feedback. With feedback enabled, a data frame acknowledged while that
        // snapshot is stale carries the update, as the real server does.
        private int deliveredResult = -1;
        private long deliveredVersion = -1;
        private volatile boolean isSchemaFeedbackEnabled;

        private SchemaHandler(int result, int tableId, long version) {
            this(result, tableId, version, true);
        }

        private SchemaHandler(int result, int tableId, long version, boolean acknowledgeData) {
            this(result, tableId, version, acknowledgeData, 2);
        }

        private SchemaHandler(
                int result,
                int tableId,
                long version,
                boolean acknowledgeData,
                int designatedIndex
        ) {
            this.result = result;
            this.tableId = tableId;
            this.version = version;
            this.acknowledgeData = acknowledgeData;
            this.designatedIndex = designatedIndex;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length >= QwpConstants.HEADER_SIZE
                    && data[QwpConstants.HEADER_OFFSET_FLAGS] == QwpSchemaProtocol.FLAG_CONTROL) {
                describeRequests.incrementAndGet();
                if (!respondToDescribe) {
                    return;
                }
                ByteBuffer request = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
                long requestId = request.getLong(QwpConstants.HEADER_SIZE + 1);
                int nameLength = request.getShort(QwpConstants.HEADER_SIZE + 1 + Long.BYTES) & 0xffff;
                byte[] name = new byte[nameLength];
                request.position(QwpConstants.HEADER_SIZE + 1 + Long.BYTES + Short.BYTES);
                request.get(name);
                String tableName = new String(name, StandardCharsets.UTF_8);
                int responseTableId = tableId + ("b".equals(tableName) ? 1 : 0);
                if ("events".equals(tableName)) {
                    deliveredResult = result;
                    deliveredVersion = version;
                }
                send(client, schemaControlFrame(schemaPayload(requestId, responseTableId)));
                return;
            }
            dataFrames.add(data);
            notifyAll();
            if (!acknowledgeData) {
                return;
            }
            send(client, ack());
        }

        private byte[] ack() {
            long sequence = ackSequence++;
            if (!isSchemaFeedbackEnabled || (result == deliveredResult && version == deliveredVersion)) {
                return QwpWireTestUtils.buildAck(sequence);
            }
            if (result != QwpSchemaProtocol.RESULT_KNOWN && result != QwpSchemaProtocol.RESULT_MISSING) {
                return ackInvalidateAll(sequence);
            }
            deliveredResult = result;
            deliveredVersion = version;
            return ackWithSchemaUpdate(sequence, "events", schemaPayload(0, tableId));
        }

        private static void send(TestWebSocketServer.ClientHandler client, byte[] frame) {
            try {
                client.sendBinary(frame);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void onOpen(TestWebSocketServer.ClientHandler client) {
            try {
                client.sendPing(new byte[]{0x51, 0x57, 0x50});
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void onPong(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length == 3 && data[0] == 0x51 && data[1] == 0x57 && data[2] == 0x50) {
                pong.countDown();
            }
        }

        private boolean awaitPong() throws InterruptedException {
            return pong.await(5, TimeUnit.SECONDS);
        }

        private synchronized byte[] awaitDataFrame() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dataFrames.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    Assert.fail("timed out waiting for data frame");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return dataFrames.get(0);
        }

        private synchronized List<byte[]> awaitDataFrames(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dataFrames.size() < count) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    Assert.fail("timed out waiting for " + count + " data frames; received " + dataFrames.size());
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return new ArrayList<>(dataFrames);
        }

        private byte[] schemaPayload(long requestId, int responseTableId) {
            byte[] id = "id".getBytes(StandardCharsets.UTF_8);
            byte[] failed = "failed_b".getBytes(StandardCharsets.UTF_8);
            byte[] ts = "ts".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1;
            if (result == QwpSchemaProtocol.RESULT_KNOWN) {
                payloadLength += 4 + 8 + 2 + 2
                        + 2 + id.length + 4 + 2
                        + 2 + failed.length + 4 + 2
                        + 2 + ts.length + 4 + 2;
            }
            ByteBuffer out = ByteBuffer.allocate(payloadLength).order(ByteOrder.LITTLE_ENDIAN);
            out.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId).put((byte) result);
            if (result == QwpSchemaProtocol.RESULT_KNOWN) {
                out.putInt(responseTableId).putLong(version).putShort((short) designatedIndex).putShort((short) 3)
                        .putShort((short) id.length).put(id).putInt(ColumnType.UUID).putShort((short) 0)
                        .putShort((short) failed.length).put(failed).putInt(ColumnType.STRING).putShort((short) 0)
                        .putShort((short) ts.length).put(ts).putInt(ColumnType.TIMESTAMP_NANO).putShort((short) 0);
            }
            return out.array();
        }
    }

    private static final class LongTextSchemaHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final List<byte[]> dataFrames = new ArrayList<>();
        private final AtomicInteger describeRequests = new AtomicInteger();
        private final int tableId;
        private volatile int targetType;
        private volatile long version;
        private long ackSequence;
        // The version the client last received, through DESCRIBE or ACK feedback.
        private long deliveredVersion = -1;

        private LongTextSchemaHandler(int tableId, long version, int targetType) {
            this.tableId = tableId;
            this.version = version;
            this.targetType = targetType;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length >= QwpConstants.HEADER_SIZE
                    && data[QwpConstants.HEADER_OFFSET_FLAGS] == QwpSchemaProtocol.FLAG_CONTROL) {
                describeRequests.incrementAndGet();
                ByteBuffer request = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
                long requestId = request.getLong(QwpConstants.HEADER_SIZE + 1);
                deliveredVersion = version;
                send(client, schemaControlFrame(schemaPayload(requestId)));
                return;
            }
            dataFrames.add(data);
            notifyAll();
            long sequence = ackSequence++;
            if (version == deliveredVersion) {
                send(client, QwpWireTestUtils.buildAck(sequence));
            } else {
                deliveredVersion = version;
                send(client, ackWithSchemaUpdate(sequence, "events", schemaPayload(0)));
            }
        }

        private static void send(TestWebSocketServer.ClientHandler client, byte[] frame) {
            try {
                client.sendBinary(frame);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private synchronized byte[] awaitDataFrame() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dataFrames.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    Assert.fail("timed out waiting for LONG-to-text data frame");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return dataFrames.get(0);
        }

        private synchronized List<byte[]> awaitDataFrames(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dataFrames.size() < count) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    Assert.fail("timed out waiting for " + count + " data frames; received " + dataFrames.size());
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return new ArrayList<>(dataFrames);
        }

        private byte[] schemaPayload(long requestId) {
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);
            byte[] failed = "failed_b".getBytes(StandardCharsets.UTF_8);
            byte[] ts = "ts".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1 + 4 + 8 + 2 + 2
                    + 2 + value.length + 4 + 2
                    + 2 + failed.length + 4 + 2
                    + 2 + ts.length + 4 + 2;
            ByteBuffer out = ByteBuffer.allocate(payloadLength).order(ByteOrder.LITTLE_ENDIAN);
            out.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId)
                    .put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                    .putInt(tableId).putLong(version).putShort((short) 2).putShort((short) 3)
                    .putShort((short) value.length).put(value).putInt(targetType).putShort((short) 0)
                    .putShort((short) failed.length).put(failed).putInt(ColumnType.UUID).putShort((short) 0)
                    .putShort((short) ts.length).put(ts).putInt(ColumnType.TIMESTAMP_NANO).putShort((short) 0);
            return out.array();
        }
    }
}
