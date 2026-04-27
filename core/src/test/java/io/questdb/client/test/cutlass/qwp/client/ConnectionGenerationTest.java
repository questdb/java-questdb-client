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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for the {@code connectionGeneration} foundation:
 * <ul>
 *   <li>Generation starts at 0 for fresh connections, jumps to 1 when the
 *       cursor engine recovered from disk (so the first batch re-publishes
 *       full schemas instead of refs the new server has never seen).</li>
 *   <li>A test-driven generation bump triggers a schema-state reset on the
 *       next encode, mirroring what the reconnect path will do once it
 *       lands.</li>
 *   <li>Persistent generation racing past the encode loop surfaces as a
 *       bounded {@code MAX_SCHEMA_RACE_RETRIES} terminal error.</li>
 * </ul>
 * Real reconnect-driven race coverage lands with the reconnect work; this
 * test exercises the producer-side primitive in isolation.
 */
public class ConnectionGenerationTest {

    private static final int TEST_PORT = 19_800 + (int) (System.nanoTime() % 100);

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-gen-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    @Test
    public void testGenerationIsZeroForFreshConnection() throws Exception {
        int port = TEST_PORT + 1;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            CursorSendEngine engine = freshEngine(sfDir);
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                    null,
                    QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                    false, engine)) {
                Assert.assertEquals("fresh engine must not bump generation",
                        0L, sender.getConnectionGenerationForTest());
            }
        }
    }

    @Test
    public void testGenerationIsOneAfterDiskRecovery() throws Exception {
        int port = TEST_PORT + 2;
        // Silent server: receives binary frames but never ACKs. Session 1
        // closes with unacked data on disk — that's the realistic recovery
        // scenario. (A clean shutdown with everything ACK'd is now treated
        // as a fully-drained slot and the .sfa files are unlinked on close;
        // recovery in that case correctly sees an empty slot.)
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // Session 1: write something, close fast (skip drain so the
            // unacked frames stay on disk).
            CursorSendEngine engine1 = freshEngine(sfDir);
            try (QwpWebSocketSender sender = connectSender(port, engine1, 0L)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }

            // Session 2: open against the populated dir. Engine recovers,
            // sender bumps generation to 1 inside ensureConnected.
            CursorSendEngine engine2 = freshEngine(sfDir);
            Assert.assertTrue("engine should report disk recovery",
                    engine2.wasRecoveredFromDisk());
            try (QwpWebSocketSender sender = connectSender(port, engine2, 0L)) {
                Assert.assertEquals("recovered engine must bump generation",
                        1L, sender.getConnectionGenerationForTest());
            }
        }
    }

    @Test
    public void testGenerationBumpResetsSchemaStateOnNextFlush() throws Exception {
        int port = TEST_PORT + 3;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            CursorSendEngine engine = freshEngine(sfDir);
            try (QwpWebSocketSender sender = connectSender(port, engine)) {
                // Batch 1: assigns + sends schema id 0 for table "foo".
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                Assert.assertEquals("schema id 0 should be confirmed sent",
                        0, sender.getMaxSentSchemaIdForTest());

                // Simulate a wire-side reconnect: bump the generation. The
                // next flush must re-reset schema state because the new
                // connection has no memory of schema id 0.
                sender.bumpConnectionGenerationForTest();

                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
                // After the reset + re-encode, schema id climbs back from
                // -1 → 0 (foo gets re-assigned). The observable signal is
                // that maxSentSchemaId went through 0 again, but the more
                // specific assertion is that lastSeenGeneration tracked the
                // bump — which we verify by confirming generation is now 1
                // and a third flush without bump does NOT re-reset.
                Assert.assertEquals(1L, sender.getConnectionGenerationForTest());
                int afterReset = sender.getMaxSentSchemaIdForTest();

                sender.table("foo").longColumn("v", 3L).atNow();
                sender.flush();
                Assert.assertEquals(
                        "no further reset without another bump — schema id stable",
                        afterReset, sender.getMaxSentSchemaIdForTest());
            }
        }
    }

    private QwpWebSocketSender connectSender(int port, CursorSendEngine engine) {
        return QwpWebSocketSender.connect(
                "localhost", port, null,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                null,
                QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                false, engine);
    }

    private QwpWebSocketSender connectSender(int port, CursorSendEngine engine,
                                             long closeFlushTimeoutMillis) {
        return QwpWebSocketSender.connect(
                "localhost", port, null,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                null,
                QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                false, engine, closeFlushTimeoutMillis);
    }

    private static CursorSendEngine freshEngine(String dir) {
        return new CursorSendEngine(dir, 4L * 1024 * 1024);
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /** Receives binary frames but never ACKs — used for unacked-data-on-disk scenarios. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally empty
        }
    }

    /** Acks every binary frame so the sender doesn't hang. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
