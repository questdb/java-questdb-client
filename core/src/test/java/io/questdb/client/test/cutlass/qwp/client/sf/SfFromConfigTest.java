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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
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

public class SfFromConfigTest {

    private static final int TEST_PORT = 19_900 + (int) (System.nanoTime() % 100);

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-config-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    @Test
    public void testFromConfigEnablesSfAndOwnsLog() throws Exception {
        int port = TEST_PORT + 1;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";store_and_forward=on;sf_dir=" + sfDir + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();
            }
            // SF dir was created by the sender via SegmentLog.open
            Assert.assertTrue("sfDir created", Files.exists(sfDir));
            // After sender close, the SegmentLog lock file should be released —
            // re-opening it must succeed.
            try (SegmentLog reopened = SegmentLog.open(sfDir, 1L << 20)) {
                Assert.assertTrue("reopen after sender close succeeds", reopened.nextSeq() >= 0);
            }
        }
    }

    @Test
    public void testStoreAndForwardOnWithoutDirRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject store_and_forward=on without sf_dir");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("requires sf_dir"));
        }
    }

    @Test
    public void testSfDirWithoutStoreAndForwardRejected() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject sf_dir without store_and_forward=on");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("store_and_forward is not enabled"));
        }
    }

    @Test
    public void testStoreAndForwardOnTcpRejected() {
        String config = "tcp::addr=localhost:9009;store_and_forward=on;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject store_and_forward on TCP");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testSfMaxBytesParsing() throws Exception {
        int port = TEST_PORT + 2;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir + ";sf_max_bytes=131072;";
            try (Sender sender = Sender.fromConfig(config)) {
                // Write enough data that segments rotate at ~128 KiB boundary.
                for (int i = 0; i < 50; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                }
                sender.flush();
            }
            // Just confirm SF dir was populated; rotation under load is exercised
            // exhaustively in SegmentLogTest.
            Assert.assertTrue("sfDir was used", Files.exists(sfDir));
        }
    }

    @Test
    public void testStoreAndForwardOffIgnoresSfDir() throws Exception {
        // Without store_and_forward=on, sf_dir isn't a valid combo (sender errors).
        // But store_and_forward=off without sf_dir should be a clean no-op.
        int port = TEST_PORT + 3;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";store_and_forward=off;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
            Assert.assertFalse("no sf dir created", Files.exists(sfDir));
        }
    }

    @Test
    public void testInvalidStoreAndForwardValueRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of invalid value");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid store_and_forward"));
        }
    }

    /**
     * SF disk-full back-pressures user thread via flush(): when the configured
     * sf_max_total_bytes is reached, flush() blocks until ACKs trim sealed
     * segments and free space. The user code never sees an error.
     */
    @Test
    public void testDiskFullBackpressureUnblocksAfterAck() throws Exception {
        int port = TEST_PORT + 4;
        // Slow-acking server: each batch acked after 1.5 s. The user thread
        // sends faster than the server can ACK, so SF disk fills before any
        // trim runs — disk-full path triggers reliably.
        DelayedAckHandler handler = new DelayedAckHandler(1_500);
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // Each per-row batch is ~30 B over the wire. With segment cap 128 B
            // and total cap 256 B, the disk fills after ~6 batches. The user
            // thread sends 20 → multiple disk-full stalls before ACKs free space.
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_max_bytes=128"
                    + ";sf_max_total_bytes=256"
                    + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                Assert.assertTrue(sender instanceof QwpWebSocketSender);
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                // Send a flood of batches faster than ACKs can drain.
                for (int i = 0; i < 20; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                    sender.flush();
                }
                long stalls = wsSender.getTotalSfDiskFullStalls();
                Assert.assertTrue(
                        "expected at least one disk-full stall, saw " + stalls,
                        stalls > 0);
            }
        }
    }

    @Test
    public void testSfFsyncParsesAndWorks() throws Exception {
        int port = TEST_PORT + 5;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // sf_fsync=on forces fsync on every append. The test mostly proves
            // the connect-string parses, the path is wired, and basic send works.
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_fsync=on;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
            }
            Assert.assertTrue(Files.exists(sfDir));
        }
    }

    @Test
    public void testInvalidSfFsyncValueRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;sf_dir=" + sfDir
                + ";sf_fsync=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid sf_fsync"));
        }
    }

    @Test
    public void testStoreAndForwardWithSyncWindowRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;sf_dir=" + sfDir
                + ";in_flight_window=1;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of SF with sync mode");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("async"));
        }
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

        // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

    /** Acks each frame after a configurable delay, on a background thread. */
    private static class DelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);

        DelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq = nextSeq.getAndIncrement();
            new Thread(() -> {
                try {
                    Thread.sleep(delayMs);
                    client.sendBinary(AckHandler.buildAck(seq));
                } catch (Exception ignored) {
                }
            }, "delayed-acker").start();
        }
    }
}
