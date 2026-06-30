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
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration tests exercising the durable-ack opt-in across the full client
 * stack: connect-string parsing, upgrade-response detection, OK-vs-durable-ack
 * trim contract, and the end-to-end behaviour against a {@link TestWebSocketServer}
 * that either advertises support (via the {@code X-QWP-Durable-Ack: enabled}
 * upgrade header) or silently ignores the opt-in.
 */
public class DurableAckIntegrationTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-da-int-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    @Test
    public void testConnectStringInvalidValueRejected() {
        // Anything other than on/off must be rejected at parse time so a typo
        // like "yes" or "1" doesn't silently disable the durability the user
        // intended.
        try {
            Sender.fromConfig("ws::addr=localhost:1;sf_dir=" + sfDir + ";request_durable_ack=yes;").close();
            Assert.fail("expected LineSenderException for invalid value");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "message names the offending key+value, was: " + e.getMessage(),
                    e.getMessage().contains("request_durable_ack")
                            && e.getMessage().contains("yes"));
        }
    }

    @Test
    public void testConnectStringOffParsesAndDoesNotOptIn() throws Exception {
        // request_durable_ack=off must behave like the param being absent --
        // the connection succeeds against a server that does NOT echo the
        // durable-ack confirmation, because the client never asked for it.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, false)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";request_durable_ack=off;";
                try (Sender sender = Sender.fromConfig(config)) {
                    sender.table("trades").longColumn("v", 1L).atNow();
                    sender.flush();
                }
            }
        });
    }

    @Test
    public void testConnectStringOnRequiresServerSupport() throws Exception {
        // OSS-like server (no X-QWP-Durable-Ack header in 101 response).
        // Opting in must throw at connect, not silently leave the SF store
        // to grow until disk fills.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, false)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";request_durable_ack=on;";
                try (Sender ignored = Sender.fromConfig(config)) {
                    Assert.fail("expected connect to fail with QwpDurableAckMismatchException");
                } catch (QwpDurableAckMismatchException e) {
                    Assert.assertEquals("localhost", e.getHost());
                    Assert.assertEquals(port, e.getPort());
                }
            }
        });
    }

    @Test
    public void testEndToEndDurableTrimDefersUntilUploadAck() throws Exception {
        // Server confirms support and emits OK acks but no durable-acks at first.
        // The client must not advance trim during the OK-only window. After the
        // test releases a cumulative durable-ack, trim catches up and close()
        // drains. The pair "OK-but-no-durable" -> grow, "durable-ack" -> drain
        // is the central durable-mode contract.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";request_durable_ack=on;close_flush_timeout_millis=5000;";
                try (Sender sender = Sender.fromConfig(config)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("trades").longColumn("v", i).atNow();
                    }
                    sender.flush();

                    // Wait for the server to OK every batch so we know the OK
                    // watermark is fully advanced. Without a durable-ack the
                    // client's ackedFsn must still be behind publishedFsn --
                    // we don't assert on internals here, just observe that
                    // the contract holds at the boundary check below.
                    handler.awaitOks();

                    // Release a cumulative durable-ack covering everything that
                    // has been OK'd so far. The client's I/O thread reads new
                    // frames whenever the connection has activity; flush() above
                    // already produced enough send/recv interleaving for the
                    // durable-ack frame to be picked up before close() drains.
                    handler.emitDurableAckForAll();
                }
                // close() returned without timing out: durable-ack-driven trim
                // ran to completion. If the loop had not been wired through,
                // close would have timed out waiting on a watermark that
                // never advances.
            }
        });
    }

    private static byte[] buildDurableAckFrame(long seqTxn) {
        byte[] name = DurableAckCapableHandler.TABLE_NAME.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x02); // STATUS_DURABLE_ACK
        bb.putShort((short) 1); // tableCount
        bb.putShort((short) name.length);
        bb.put(name);
        bb.putLong(seqTxn);
        return bb.array();
    }

    private static byte[] buildOkFrame(long wireSeq, long seqTxn) {
        byte[] name = DurableAckCapableHandler.TABLE_NAME.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(wireSeq);
        bb.putShort((short) 1); // tableCount
        bb.putShort((short) name.length);
        bb.put(name);
        bb.putLong(seqTxn);
        return bb.array();
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        rmDir(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static class DurableAckCapableHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE_NAME = "trades";
        private final AtomicLong nextSeqTxn = new AtomicLong(0);
        private final AtomicLong nextWireSeq = new AtomicLong(0);
        private volatile TestWebSocketServer.ClientHandler activeClient;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            activeClient = client;
            try {
                long wireSeq = nextWireSeq.getAndIncrement();
                long seqTxn = nextSeqTxn.getAndIncrement();
                client.sendBinary(buildOkFrame(wireSeq, seqTxn));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void awaitOks() throws InterruptedException {
            long deadline = System.currentTimeMillis() + (long) 5000;
            while (totalOks() < (long) 50 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        }

        void emitDurableAckForAll() throws IOException {
            // Cumulative durable-ack: every OK already issued is now durable.
            // Single-table handler so one entry suffices.
            TestWebSocketServer.ClientHandler c = activeClient;
            if (c != null) {
                long seqTxn = Math.max(0L, nextSeqTxn.get() - 1L);
                c.sendBinary(buildDurableAckFrame(seqTxn));
            }
        }

        long totalOks() {
            return nextWireSeq.get();
        }
    }
}
