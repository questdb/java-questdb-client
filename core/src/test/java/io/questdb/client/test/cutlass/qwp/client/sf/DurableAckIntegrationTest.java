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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
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
    public void testBooleanConnectOverloadRequestsLegacyTier() throws Exception {
        // The boolean connect overloads map true onto the legacy request:
        // the upgrade header carries "true" and the "enabled" grant is
        // accepted. Callers linked against the boolean signatures keep the
        // pre-tier wire behavior. (ExportedApiCompatibilityTest pins the
        // signatures; this pins what they do.)
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();

                CursorSendEngine engine = new CursorSendEngine(sfDir, 16384);
                try (Sender ignored = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, true, engine)) {
                    Assert.assertEquals("true",
                            server.pollDurableAckRequest(5, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test
    public void testBuilderInvalidTierValueRejected() {
        // The programmatic CharSequence overload applies the same parse as the
        // config string: a typo must throw, naming the key and the value.
        try {
            Sender.builder(Sender.Transport.WEBSOCKET).requestDurableAck("yes");
            Assert.fail("expected LineSenderException for invalid tier value");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "message names the offending key+value, was: " + e.getMessage(),
                    e.getMessage().contains("request_durable_ack")
                            && e.getMessage().contains("yes"));
        }
    }

    @Test
    public void testBuilderRejectsTiersOnHttpTransport() {
        // Durable-ack streams exist only on the WebSocket transport; asking
        // for a tier on an HTTP builder must fail fast at configuration time.
        try {
            Sender.builder(Sender.Transport.HTTP).requestDurableAck("local");
            Assert.fail("expected LineSenderException for HTTP transport");
        } catch (LineSenderException e) {
            Assert.assertTrue("was: " + e.getMessage(),
                    e.getMessage().contains("only supported for WebSocket"));
        }
    }

    @Test
    public void testConnectStringLocalRequiresServerSupport() throws Exception {
        // Tier requests keep the all-or-nothing contract of the legacy opt-in:
        // a server that does not confirm the durable-ack grant (no
        // X-QWP-Durable-Ack header) must fail the connect, not leave the
        // store-and-forward log growing while waiting on acks that never come.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, false)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";request_durable_ack=local;";
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
    public void testLocalRequestDeniedWhenServerGrantsDifferentSet() throws Exception {
        // A server that answers a "local" request with the legacy "enabled"
        // token granted a set other than the one requested. The client must
        // read any token except its expected one as a denial -- trimming on a
        // foreign grant could otherwise drop data on a guarantee weaker than
        // the caller configured.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.setDurableAckHeaderValue("enabled");
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";request_durable_ack=local;";
                try (Sender ignored = Sender.fromConfig(config)) {
                    Assert.fail("expected connect to fail with QwpDurableAckMismatchException");
                } catch (QwpDurableAckMismatchException e) {
                    Assert.assertEquals(port, e.getPort());
                }
            }
        });
    }

    @Test
    public void testRequestHeaderCarriesConfiguredTierSet() throws Exception {
        // The upgrade request must carry the exact token for each configured
        // set: the legacy "on" travels as "true" (the request value
        // tier-unaware servers recognize), explicit sets travel verbatim.
        // No rows are sent, so close() returns without waiting on acks.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();

                String[][] cases = {
                        {"on", "true"},
                        {"local", "local"},
                        {"replicated", "replicated"},
                        {"local,replicated", "local,replicated"},
                };
                for (String[] c : cases) {
                    String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                            + ";request_durable_ack=" + c[0] + ";";
                    Sender.fromConfig(config).close();
                    Assert.assertEquals("config value " + c[0],
                            c[1], server.pollDurableAckRequest(5, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test
    public void testEndToEndBothTiersLocalAckIsProgressOnly() throws Exception {
        // request_durable_ack=local,replicated: the replicated ack is the trim
        // trigger; local acks surface early per-table progress without popping
        // anything. The loop's counters and the per-table local watermark are
        // the observable surface for that split.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";request_durable_ack=local,replicated;close_flush_timeout_millis=5000;";
                try (Sender sender = Sender.fromConfig(config)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("trades").longColumn("v", i).atNow();
                    }
                    sender.flush(); // one batch -> one OK
                    handler.awaitOkBatches(1);
                    long batches = 1;

                    CursorWebSocketSendLoop loop =
                            ((QwpWebSocketSender) sender).cursorSendLoopForTest();
                    Assert.assertNotNull(loop);

                    // Release local acks covering everything OK'd, then nudge
                    // the connection with extra rows until the I/O thread has
                    // observed one -- the local ack alone must not trim.
                    handler.emitLocalDurableAckForAll();
                    long deadline = System.currentTimeMillis() + 5000;
                    while (loop.getTotalLocalDurableAcks() == 0
                            && System.currentTimeMillis() < deadline) {
                        sender.table("trades").longColumn("v", -1L).atNow();
                        sender.flush();
                        batches++;
                        Thread.sleep(10);
                    }
                    Assert.assertTrue("local ack never observed",
                            loop.getTotalLocalDurableAcks() > 0);
                    Assert.assertTrue("local watermark tracks the fsync frontier",
                            loop.getLocalDurableTableWatermark("trades") >= 0);
                    Assert.assertEquals(
                            "local acks must not advance trim when replicated is requested",
                            0L, loop.getTotalDurableTrimAdvances());

                    // Cover every batch sent (nudges included) with a
                    // replicated ack so close() drains on the actual trim
                    // trigger.
                    handler.awaitOkBatches(batches);
                    handler.emitDurableAckForAll();
                }
            }
        });
    }

    @Test
    public void testEndToEndLocalTrimDefersUntilLocalAck() throws Exception {
        // request_durable_ack=local: OK frames alone never trim; the
        // STATUS_LOCAL_DURABLE_ACK stream is the trim trigger. close() drains
        // only once the local ack covers everything sent -- the local-tier
        // mirror of testEndToEndDurableTrimDefersUntilUploadAck.
        TestUtils.assertMemoryLeak(() -> {
            DurableAckCapableHandler handler = new DurableAckCapableHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                int port = server.getPort();
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";request_durable_ack=local;close_flush_timeout_millis=5000;";
                try (Sender sender = Sender.fromConfig(config)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("trades").longColumn("v", i).atNow();
                    }
                    sender.flush(); // one batch -> one OK
                    handler.awaitOkBatches(1);
                    handler.emitLocalDurableAckForAll();
                }
                // close() returned without timing out: the local ack drove the
                // trim to completion.
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

                    // Wait for the server to OK the flushed batch so we know
                    // the OK watermark is fully advanced. Without a durable-ack
                    // the client's ackedFsn must still be behind publishedFsn --
                    // we don't assert on internals here, just observe that
                    // the contract holds at the boundary check below.
                    handler.awaitOkBatches(1);

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

    private static byte[] buildDurableAckFrame(byte status, long seqTxn) {
        // STATUS_DURABLE_ACK and STATUS_LOCAL_DURABLE_ACK share the layout:
        // status(1) + tableCount(2) + nameLen(2) + name + seqTxn(8).
        byte[] name = DurableAckCapableHandler.TABLE_NAME.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.put(status);
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

        void awaitOkBatches(long count) throws InterruptedException {
            // One OK frame per QWP batch (i.e. per flush), NOT per row. A
            // silent return on timeout would let the caller proceed on a
            // watermark that never advanced, so this fails loudly instead.
            long deadline = System.currentTimeMillis() + (long) 5000;
            while (totalOks() < count && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue(
                    "server never OK'd " + count + " batch(es), got " + totalOks(),
                    totalOks() >= count);
        }

        void emitDurableAckForAll() throws IOException {
            // Cumulative durable-ack: every OK already issued is now durable.
            // Single-table handler so one entry suffices.
            emitAckForAll(WebSocketResponse.STATUS_DURABLE_ACK);
        }

        void emitLocalDurableAckForAll() throws IOException {
            // Cumulative local-durability ack: every OK already issued is now
            // fdatasync-durable on the "server".
            emitAckForAll(WebSocketResponse.STATUS_LOCAL_DURABLE_ACK);
        }

        private void emitAckForAll(byte status) throws IOException {
            TestWebSocketServer.ClientHandler c = activeClient;
            if (c != null) {
                long seqTxn = Math.max(0L, nextSeqTxn.get() - 1L);
                c.sendBinary(buildDurableAckFrame(status, seqTxn));
            }
        }

        long totalOks() {
            return nextWireSeq.get();
        }
    }
}
