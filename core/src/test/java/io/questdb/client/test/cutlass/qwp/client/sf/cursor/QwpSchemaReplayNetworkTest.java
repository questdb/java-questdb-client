/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.Sender;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpSchemaCapabilityMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import io.questdb.client.test.tools.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class QwpSchemaReplayNetworkTest {
    private static final int SEGMENT_SIZE = 1 << 20;
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().parentFolder(taskTempRoot()).build();

    @Test
    public void testBackgroundDrainerRetiresOrphanSchemaTailWithoutConnecting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            File root = temp.newFolder("drainer-orphan-schema-tail");
            String slot = new File(root, "ghost").getAbsolutePath();
            seedDeferredSchemaTail(slot);
            RecordingHandler oldHandler = new RecordingHandler(false, "deferred");
            try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
                oldPeer.start();
                Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
                IgnoringFactory factory = new IgnoringFactory(oldPeer.getPort(), 1);
                BackgroundDrainer drainer = new BackgroundDrainer(
                        slot, SEGMENT_SIZE, 4L * SEGMENT_SIZE, factory,
                        5_000, 1, 4, false, 0
                );
                Thread thread = new Thread(drainer::run, "schema-orphan-tail-drainer");
                thread.start();
                try {
                    thread.join(5_000);
                    Assert.assertFalse("drainer did not finish", thread.isAlive());
                } finally {
                    drainer.requestStop();
                    thread.join(5_000);
                }
                // The aborted tail is retired locally, so the drainer never needs a peer.
                Assert.assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                Assert.assertEquals(0, factory.attempts.get());
                Assert.assertEquals(0, oldHandler.frames.size());
            }
            assertNoQuarantine(root);
        });
    }

    @Test
    public void testBackgroundDrainerRejectsUnconfirmedClientReturnedByFactoryWithoutQuarantine() throws Exception {
        File root = temp.newFolder("drainer-old-peer");
        String slot = new File(root, "ghost").getAbsolutePath();
        seedMixed(slot);
        RecordingHandler oldHandler = new RecordingHandler(false, "replay");
        try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
            oldPeer.start();
            Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
            IgnoringFactory factory = new IgnoringFactory(oldPeer.getPort(), 2);
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slot, SEGMENT_SIZE, 4L * SEGMENT_SIZE, factory,
                    5_000, 1, 4, false, 0
            );
            AtomicInteger errors = new AtomicInteger();
            drainer.setErrorSink(error -> errors.incrementAndGet());
            Thread thread = new Thread(drainer::run, "schema-replay-drainer");
            thread.start();
            try {
                Assert.assertTrue("factory must return at least two unconfirmed clients",
                        factory.awaitAttempts(5, TimeUnit.SECONDS));
            } finally {
                drainer.requestStop();
                thread.join(5_000);
            }
            Assert.assertFalse("drainer did not stop", thread.isAlive());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
            Assert.assertEquals(0, oldHandler.frames.size());
            Assert.assertEquals(0, errors.get());
        }
        assertRetained(slot, 1);
        assertNoQuarantine(root);
    }

    @Test(timeout = 20_000)
    public void testAckedSchemaFramesDoNotBlockLegacyPeerAfterDowngrade() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            File root = temp.newFolder("acked-then-downgrade");
            String slot = new File(root, "default").getAbsolutePath();
            RecordingHandler supportingHandler = new RecordingHandler(true, "downgrade");
            RecordingHandler legacyHandler = new RecordingHandler(true, "downgrade");
            try (TestWebSocketServer legacyPeer = new TestWebSocketServer(legacyHandler);
                 CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_SIZE);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer schemaRows = table("downgrade", 1);
                 QwpTableBuffer legacyRows = table("downgrade", 2)) {
                legacyPeer.start();
                Assert.assertTrue(legacyPeer.awaitStart(5, TimeUnit.SECONDS));
                SchemaAwareFactory factory = new SchemaAwareFactory(legacyPeer.getPort(), 1);
                CursorWebSocketSendLoop loop = null;
                try {
                    try (TestWebSocketServer supportingPeer = new TestWebSocketServer(supportingHandler)) {
                        supportingPeer.setAdvertiseSchema(true);
                        supportingPeer.start();
                        Assert.assertTrue(supportingPeer.awaitStart(5, TimeUnit.SECONDS));
                        WebSocketClient initialClient = connectLegacyClient(supportingPeer.getPort(), true);
                        loop = new CursorWebSocketSendLoop(
                                initialClient, engine, 0, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                                factory, 1, 4, false);
                        Assert.assertTrue(initialClient.isQwpSchemaEnabled());
                        loop.start();
                        int length = encoder.encodeSchema(schemaRows, -1, -1);
                        Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                        Assert.assertTrue(engine.requiresSchema());
                        awaitAckedFsn(engine, 0);
                        Assert.assertFalse("an acked schema frame must not pin the capability",
                                engine.requiresSchema());
                    }
                    // The schema-capable peer is gone; the loop must accept the legacy peer.
                    int length = encoder.encode(legacyRows);
                    Assert.assertEquals(1, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                    awaitAckedFsn(engine, 1);
                    Assert.assertNull(loop.getTerminalError());
                    Assert.assertEquals(1, supportingHandler.frames.size());
                    Assert.assertEquals(1, legacyHandler.frames.size());
                    Assert.assertEquals(0, legacyHandler.frames.get(0)[QwpConstants.HEADER_OFFSET_FLAGS]
                            & QwpConstants.FLAG_SCHEMA);
                } finally {
                    if (loop != null) {
                        loop.close();
                    }
                }
            }
        });
    }

    @Test
    public void testOldPeerSeesNoBytesThenSupportingPeerReplaysExactMixedFrames() throws Exception {
        File root = temp.newFolder("mixed");
        String slot = new File(root, "default").getAbsolutePath();
        byte[][] expected = seedMixed(slot);

        RecordingHandler oldHandler = new RecordingHandler(false, "replay");
        try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
            oldPeer.start();
            Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertThrows(QwpSchemaCapabilityMismatchException.class,
                    () -> Sender.fromConfig(config(oldPeer.getPort(), root, true)));
            Assert.assertEquals(0, oldHandler.frames.size());
        }
        assertRetained(slot, 1);
        assertNoQuarantine(root);

        RecordingHandler supportingHandler = new RecordingHandler(true, "replay");
        try (TestWebSocketServer supportingPeer = new TestWebSocketServer(supportingHandler)) {
            supportingPeer.setAdvertiseSchema(true);
            supportingPeer.start();
            Assert.assertTrue(supportingPeer.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(supportingPeer.getPort(), root, false))) {
                Assert.assertTrue(sender.drain(10_000));
            }
        }
        Assert.assertEquals(2, supportingHandler.frames.size());
        Assert.assertArrayEquals(expected[0], supportingHandler.frames.get(0));
        Assert.assertArrayEquals(expected[1], supportingHandler.frames.get(1));
        assertNoQuarantine(root);
    }

    @Test(timeout = 15_000)
    public void testOffReplaysRecoveredSchemaFramesInForegroundAndOrphan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (boolean isOrphan : new boolean[]{false, true}) {
                File root = temp.newFolder(isOrphan ? "off-orphan" : "off-foreground");
                String slot = new File(root, isOrphan ? "ghost" : "default").getAbsolutePath();
                byte[][] expected = seedMixed(slot);
                RecordingHandler handler = new RecordingHandler(true, "replay");
                try (TestWebSocketServer peer = new TestWebSocketServer(handler)) {
                    peer.setAdvertiseSchema(true);
                    peer.start();
                    Assert.assertTrue(peer.awaitStart(5, TimeUnit.SECONDS));
                    if (isOrphan) {
                        // The owner itself has no schema backlog; only this drainer opts in.
                        try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(
                                "ws::addr=localhost:" + peer.getPort() + ";schema_mode=off;")) {
                            Assert.assertFalse(peer.hasRequestedSchema());
                            BackgroundDrainer drainer = new BackgroundDrainer(
                                    slot, SEGMENT_SIZE, 4L * SEGMENT_SIZE,
                                    sender.newBackgroundReconnectFactory(() -> false),
                                    5_000, 1, 4, false, 0);
                            Thread thread = new Thread(drainer, "off-schema-orphan");
                            thread.start();
                            try {
                                thread.join(5_000);
                                Assert.assertFalse("drainer did not finish", thread.isAlive());
                                Assert.assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                            } finally {
                                drainer.requestStop();
                                thread.join(5_000);
                            }
                        }
                    } else {
                        try (Sender sender = Sender.fromConfig(config(peer.getPort(), root, false)
                                + "schema_mode=off;")) {
                            Assert.assertTrue(sender.drain(5_000));
                        }
                    }
                    Assert.assertTrue(peer.hasRequestedSchema());
                }
                Assert.assertEquals(2, handler.frames.size());
                Assert.assertArrayEquals(expected[0], handler.frames.get(0));
                Assert.assertArrayEquals(expected[1], handler.frames.get(1));
                assertNoQuarantine(root);
            }
        });
    }

    @Test
    public void testPublicSenderSchemaFrameSurvivesRestartByteForByte() throws Exception {
        File root = temp.newFolder("public-sender-restart");
        String slot = new File(root, "default").getAbsolutePath();
        SchemaRecordingHandler firstHandler = new SchemaRecordingHandler(false);
        byte[] expected;
        try (TestWebSocketServer firstPeer = new TestWebSocketServer(firstHandler)) {
            firstPeer.setAdvertiseSchema(true);
            firstPeer.start();
            Assert.assertTrue(firstPeer.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(firstPeer.getPort(), root, false))) {
                sender.table("small_replay").byteColumn("value", (byte) -7).atNow();
                Assert.assertEquals(0, sender.flushAndGetSequence());
                expected = firstHandler.awaitDataFrame();
            }
        }
        assertRetained(slot, 0);

        SchemaRecordingHandler replayHandler = new SchemaRecordingHandler(true);
        try (TestWebSocketServer replayPeer = new TestWebSocketServer(replayHandler)) {
            replayPeer.setAdvertiseSchema(true);
            replayPeer.start();
            Assert.assertTrue(replayPeer.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(replayPeer.getPort(), root, false))) {
                Assert.assertTrue(sender.drain(10_000));
            }
        }
        Assert.assertArrayEquals(expected, replayHandler.awaitDataFrame());
        Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                expected[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
        assertNoQuarantine(root);
    }

    @Test
    public void testDeferredOnlySchemaTailIsRetainedByOldPeerThenAbortedLocally() throws Exception {
        File root = temp.newFolder("deferred");
        String slot = new File(root, "default").getAbsolutePath();
        seedDeferredSchemaTail(slot);

        RecordingHandler oldHandler = new RecordingHandler(false, "deferred");
        try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
            oldPeer.start();
            Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertThrows(QwpSchemaCapabilityMismatchException.class,
                    () -> Sender.fromConfig(config(oldPeer.getPort(), root, false)));
            Assert.assertEquals(0, oldHandler.frames.size());
        }
        assertRetained(slot, 0);
        assertNoQuarantine(root);

        RecordingHandler supportingHandler = new RecordingHandler(true, "deferred");
        try (TestWebSocketServer supportingPeer = new TestWebSocketServer(supportingHandler)) {
            supportingPeer.setAdvertiseSchema(true);
            supportingPeer.start();
            Assert.assertTrue(supportingPeer.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(supportingPeer.getPort(), root, false))) {
                Assert.assertTrue(sender.drain(10_000));
            }
        }
        Assert.assertEquals("an uncommitted recovered tail must be aborted, not transmitted", 0, supportingHandler.frames.size());
        assertNoQuarantine(root);
    }

    @Test
    public void testLiveSchemaAppendIsRetainedWhenConnectedPeerIsLegacyOnly() throws Exception {
        File root = temp.newFolder("live-append-old-peer");
        String slot = new File(root, "default").getAbsolutePath();
        RecordingHandler oldHandler = new RecordingHandler(false, "live_append");
        try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
            oldPeer.start();
            Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
            WebSocketClient initialClient = connectLegacyClient(oldPeer.getPort(), false);
            SchemaAwareFactory factory = new SchemaAwareFactory(oldPeer.getPort(), 2);
            CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_SIZE);
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    initialClient, engine, 0, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    factory, 1, 4, false);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = table("live_append", 7)) {
                loop.start();
                int length = encoder.encodeSchema(table, -1, -1);
                Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
                Assert.assertTrue("schema-required reconnects must remain bounded and observable",
                        factory.awaitAttempts(5, TimeUnit.SECONDS));
                Assert.assertTrue(loop.isRunning());
                Assert.assertNull(loop.getTerminalError());
                Assert.assertEquals("no extended frame may reach a legacy peer", 0, oldHandler.frames.size());
                Assert.assertEquals(-1, engine.ackedFsn());
                Assert.assertEquals(0, engine.publishedFsn());
            } finally {
                loop.close();
                engine.close();
            }
        }
        assertRetained(slot, 0);
        assertNoQuarantine(root);
    }

    @Test
    public void testSchemaFeedbackAckAdvancesRecoveredBacklog() throws Exception {
        File root = temp.newFolder("feedback-ack");
        seedMixed(new File(root, "default").getAbsolutePath());
        FeedbackHandler handler = new FeedbackHandler(false);
        try (TestWebSocketServer peer = new TestWebSocketServer(handler)) {
            peer.setAdvertiseSchema(true);
            peer.start();
            Assert.assertTrue(peer.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(peer.getPort(), root, false))) {
                Assert.assertTrue(sender.drain(10_000));
            }
        }
        Assert.assertEquals(2, handler.frames.get());
        assertNoQuarantine(root);
    }

    @Test
    public void testMalformedSchemaFeedbackDoesNotAdvanceRecoveredBacklog() throws Exception {
        File root = temp.newFolder("feedback-malformed");
        String slot = new File(root, "default").getAbsolutePath();
        seedMixed(slot);
        FeedbackHandler handler = new FeedbackHandler(true);
        try (TestWebSocketServer peer = new TestWebSocketServer(handler)) {
            peer.setAdvertiseSchema(true);
            peer.start();
            Assert.assertTrue(peer.awaitStart(5, TimeUnit.SECONDS));
            Sender sender = Sender.fromConfig(config(peer.getPort(), root, false));
            try {
                Assert.assertTrue("malformed feedback must be parsed, rejected, and trigger replay",
                        handler.awaitDistinctClients(5, TimeUnit.SECONDS));
            } finally {
                sender.close();
            }
        }
        Assert.assertTrue(handler.frames.get() > 0);
        assertRetained(slot, 1);
        assertNoQuarantine(root);
    }

    @Test
    public void testInvalidSchemaColumnsDoNotAdvanceRecoveredBacklog() throws Exception {
        File root = temp.newFolder("feedback-invalid-columns");
        String slot = new File(root, "default").getAbsolutePath();
        seedMixed(slot);
        FeedbackHandler handler = new FeedbackHandler(false, true);
        try (TestWebSocketServer peer = new TestWebSocketServer(handler)) {
            peer.setAdvertiseSchema(true);
            peer.start();
            Assert.assertTrue(peer.awaitStart(5, TimeUnit.SECONDS));
            Sender sender = Sender.fromConfig(config(peer.getPort(), root, false));
            try {
                Assert.assertTrue("invalid schema feedback must be rejected and replayed",
                        handler.awaitDistinctClients(5, TimeUnit.SECONDS));
            } finally {
                sender.close();
            }
        }
        Assert.assertTrue(handler.frames.get() > 0);
        assertRetained(slot, 1);
        assertNoQuarantine(root);
    }

    @Test
    public void testSchemaRequirementIsIsolatedAcrossForegroundAndBackgroundFactories() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingHandler oldHandler = new RecordingHandler(false, "unused");
            try (TestWebSocketServer oldPeer = new TestWebSocketServer(oldHandler)) {
                oldPeer.start();
                Assert.assertTrue(oldPeer.awaitStart(5, TimeUnit.SECONDS));
                assertSchemaRequirementIsolated(oldPeer.getPort(), true);
                assertSchemaRequirementIsolated(oldPeer.getPort(), false);
                Assert.assertEquals(0, oldHandler.frames.size());
            }
        });
    }

    private static void assertNoQuarantine(File root) {
        File[] files = root.listFiles();
        Assert.assertNotNull(files);
        for (File file : files) {
            Assert.assertFalse(file.getName(), file.getName().contains(".unreplayable-"));
            Assert.assertFalse(file.getName(), file.getName().equals(OrphanScanner.FAILED_SENTINEL_NAME));
            File failed = new File(file, OrphanScanner.FAILED_SENTINEL_NAME);
            Assert.assertFalse(failed.getAbsolutePath(), failed.exists());
        }
    }

    private static WebSocketClient connectLegacyClient(int port, boolean requestSchema) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.connect("localhost", port);
            if (requestSchema) {
                client.requestQwpSchema();
            }
            client.upgrade("/write/v4", 5_000, null);
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    private static void assertRetained(String slot, long publishedFsn) {
        try (CursorSendEngine recovered = new CursorSendEngine(slot, SEGMENT_SIZE)) {
            Assert.assertTrue(recovered.requiresSchema());
            Assert.assertEquals(-1, recovered.ackedFsn());
            Assert.assertEquals(publishedFsn, recovered.publishedFsn());
        }
    }

    private static void assertSchemaRequirementIsolated(int port, boolean foregroundFirst) throws Exception {
        try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(
                "ws::addr=localhost:" + port + ';')) {
            CursorWebSocketSendLoop.ReconnectFactory foreground;
            CursorWebSocketSendLoop.ReconnectFactory background;
            if (foregroundFirst) {
                foreground = sender.newReconnectFactory();
                background = sender.newBackgroundReconnectFactory(() -> false);
                foreground.setSchemaRequired(true);
                assertSchemaMismatch(foreground);
                try (WebSocketClient client = background.reconnect()) {
                    Assert.assertFalse(client.isQwpSchemaEnabled());
                }
            } else {
                background = sender.newBackgroundReconnectFactory(() -> false);
                foreground = sender.newReconnectFactory();
                background.setSchemaRequired(true);
                assertSchemaMismatch(background);
                try (WebSocketClient client = foreground.reconnect()) {
                    Assert.assertFalse(client.isQwpSchemaEnabled());
                }
            }
        }
    }

    private static void assertSchemaMismatch(CursorWebSocketSendLoop.ReconnectFactory factory) throws Exception {
        WebSocketClient client = null;
        try {
            client = factory.reconnect();
            Assert.fail("an old peer must be rejected by the schema-required factory");
        } catch (QwpSchemaCapabilityMismatchException expected) {
            // Expected: the requirement belongs to this factory's stream.
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long fsn) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (engine.ackedFsn() < fsn) {
            if (System.nanoTime() > deadline) {
                Assert.fail("timed out waiting for ackedFsn " + fsn + ", actual " + engine.ackedFsn());
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private static String config(int port, File root, boolean durableAck) {
        return "ws::addr=localhost:" + port
                + ";sf_dir=" + root.getAbsolutePath()
                + ";sender_id=default;sf_max_segment_bytes=1m;request_durable_ack="
                + (durableAck ? "on" : "off") + ";close_flush_timeout_millis=0;";
    }

    private static byte[] copy(QwpWebSocketEncoder encoder, int length) {
        byte[] bytes = new byte[length];
        long address = encoder.getBuffer().getBufferPtr();
        for (int i = 0; i < length; i++) {
            bytes[i] = io.questdb.client.std.Unsafe.getUnsafe().getByte(address + i);
        }
        return bytes;
    }

    private static byte[][] seedMixed(String slot) {
        try (CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_SIZE);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer legacy = table("replay", 1);
             QwpTableBuffer schema = table("replay", 2)) {
            byte[] first = copy(encoder, encoder.encode(legacy));
            Assert.assertEquals(0, first[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), first.length));
            byte[] second = copy(encoder, encoder.encodeSchema(schema, -1, -1));
            Assert.assertEquals(QwpConstants.FLAG_SCHEMA, second[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_SCHEMA);
            Assert.assertEquals(1, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), second.length));
            return new byte[][]{first, second};
        }
    }

    private static void seedDeferredSchemaTail(String slot) {
        try (CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_SIZE);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer table = table("deferred", 1)) {
            encoder.setDeferCommit(true);
            int length = encoder.encodeSchema(table, -1, -1);
            Assert.assertEquals(0, engine.appendBlocking(encoder.getBuffer().getBufferPtr(), length));
        }
    }

    private static QwpTableBuffer table(String name, long value) {
        QwpTableBuffer table = new QwpTableBuffer(name);
        table.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(value);
        table.nextRow();
        return table;
    }

    private static File taskTempRoot() {
        File root = new File("target/schema-replay-network").getAbsoluteFile();
        Assert.assertTrue(root.exists() || root.mkdirs());
        return root;
    }

    private static final class RecordingHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final boolean acknowledge;
        private final List<byte[]> frames = new ArrayList<>();
        private final String tableName;

        private RecordingHandler(boolean acknowledge, String tableName) {
            this.acknowledge = acknowledge;
            this.tableName = tableName;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int sequence = frames.size();
            frames.add(data);
            if (acknowledge) {
                try {
                    client.sendBinary(ok(sequence));
                    client.sendBinary(durable(sequence));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }

        private byte[] durable(long seqTxn) {
            byte[] name = tableName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(WebSocketResponse.STATUS_DURABLE_ACK).putShort((short) 1).putShort((short) name.length).put(name).putLong(seqTxn);
            return buffer.array();
        }

        private static byte[] ok(long sequence) {
            return ByteBuffer.allocate(11).order(ByteOrder.LITTLE_ENDIAN)
                    .put(WebSocketResponse.STATUS_OK).putLong(sequence).putShort((short) 0).array();
        }
    }

    private static final class SchemaRecordingHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final boolean acknowledge;
        private final List<byte[]> dataFrames = new ArrayList<>();

        private SchemaRecordingHandler(boolean acknowledge) {
            this.acknowledge = acknowledge;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length >= QwpConstants.HEADER_SIZE
                    && data[QwpConstants.HEADER_OFFSET_FLAGS] == QwpSchemaProtocol.FLAG_CONTROL) {
                ByteBuffer request = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
                sendSchema(client, request.getLong(QwpConstants.HEADER_SIZE + 1));
                return;
            }
            dataFrames.add(data);
            notifyAll();
            if (acknowledge) {
                try {
                    client.sendBinary(RecordingHandler.ok(0));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }

        private synchronized byte[] awaitDataFrame() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dataFrames.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    Assert.fail("timed out waiting for schema data frame");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return dataFrames.get(0);
        }

        private static void sendSchema(TestWebSocketServer.ClientHandler client, long requestId) {
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);
            byte[] timestamp = "ts".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + Long.BYTES + 1 + Integer.BYTES + Long.BYTES
                    + Short.BYTES + Short.BYTES
                    + Short.BYTES + value.length + Integer.BYTES + Short.BYTES
                    + Short.BYTES + timestamp.length + Integer.BYTES + Short.BYTES;
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
                    .putInt(301)
                    .putLong(401)
                    .putShort((short) 1)
                    .putShort((short) 2)
                    .putShort((short) value.length)
                    .put(value)
                    .putInt(ColumnType.INT)
                    .putShort((short) 0)
                    .putShort((short) timestamp.length)
                    .put(timestamp)
                    .putInt(ColumnType.TIMESTAMP_NANO)
                    .putShort((short) 0);
            try {
                client.sendBinary(response.array());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static final class IgnoringFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch attemptsLatch;
        private final int port;

        private IgnoringFactory(int port, int expectedAttempts) {
            this.port = port;
            this.attemptsLatch = new CountDownLatch(expectedAttempts);
        }

        boolean awaitAttempts(long timeout, TimeUnit unit) throws InterruptedException {
            return attemptsLatch.await(timeout, unit);
        }

        @Override
        public WebSocketClient reconnect() {
            attempts.incrementAndGet();
            attemptsLatch.countDown();
            WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
            boolean success = false;
            try {
                client.connect("localhost", port);
                client.upgrade("/write/v4", 5_000, null);
                Assert.assertFalse(client.isQwpSchemaEnabled());
                success = true;
                return client;
            } finally {
                if (!success) {
                    client.close();
                }
            }
        }

        @Override
        public void setSchemaRequired(boolean isRequired) {
            // Deliberately ignored: returned-client validation must still reject it.
        }
    }

    private static final class FeedbackHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger frames = new AtomicInteger();
        private final CountDownLatch distinctClients;
        private final boolean malformed;
        private final boolean invalidColumns;
        private TestWebSocketServer.ClientHandler lastClient;

        private FeedbackHandler(boolean malformed) {
            this(malformed, false);
        }

        private FeedbackHandler(boolean malformed, boolean invalidColumns) {
            this.malformed = malformed;
            this.invalidColumns = invalidColumns;
            this.distinctClients = new CountDownLatch(malformed || invalidColumns ? 2 : 1);
        }

        private boolean awaitDistinctClients(long timeout, TimeUnit unit) throws InterruptedException {
            return distinctClients.await(timeout, unit);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int sequence = frames.getAndIncrement();
            byte[] response = schemaFeedbackOk(sequence);
            if (malformed) {
                response = java.util.Arrays.copyOf(response, response.length - 1);
            } else if (invalidColumns) {
                response = schemaFeedbackInvalidColumns(sequence);
            }
            try {
                client.sendBinary(response);
                if (client != lastClient) {
                    lastClient = client;
                    distinctClients.countDown();
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private static byte[] schemaFeedbackOk(long sequence) {
            byte[] tableName = "replay".getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(11 + 2 + 2 + tableName.length + 4 + 10)
                    .order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES)
                    .putLong(sequence).putShort((short) 0).putShort((short) 1)
                    .putShort((short) tableName.length).put(tableName).putInt(10)
                    .put((byte) 2).putLong(0).put((byte) 1);
            return buffer.array();
        }

        private static byte[] schemaFeedbackInvalidColumns(long sequence) {
            byte[] tableName = "replay".getBytes(StandardCharsets.UTF_8);
            byte[] column = "x".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 26 + 2 * (2 + column.length + 6);
            ByteBuffer buffer = ByteBuffer.allocate(11 + 2 + 2 + tableName.length + 4 + payloadLength)
                    .order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES)
                    .putLong(sequence).putShort((short) 0).putShort((short) 1)
                    .putShort((short) tableName.length).put(tableName).putInt(payloadLength)
                    .put((byte) 2).putLong(0).put((byte) 0).putInt(1).putLong(1)
                    .putShort((short) -1).putShort((short) 2);
            for (byte name : new byte[]{'x', 'X'}) {
                buffer.putShort((short) 1).put(name).putInt(5).putShort((short) 0);
            }
            return buffer.array();
        }
    }

    private static final class SchemaAwareFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final CountDownLatch attemptsLatch;
        private final int port;
        private volatile boolean isSchemaRequired;

        private SchemaAwareFactory(int port, int expectedAttempts) {
            this.port = port;
            this.attemptsLatch = new CountDownLatch(expectedAttempts);
        }

        boolean awaitAttempts(long timeout, TimeUnit unit) throws InterruptedException {
            return attemptsLatch.await(timeout, unit);
        }

        @Override
        public WebSocketClient reconnect() {
            attemptsLatch.countDown();
            return connectLegacyClient(port, isSchemaRequired);
        }

        @Override
        public void setSchemaRequired(boolean isRequired) {
            isSchemaRequired = isRequired;
        }
    }
}
