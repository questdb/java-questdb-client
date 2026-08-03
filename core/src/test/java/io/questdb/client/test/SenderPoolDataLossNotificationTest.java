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

package io.questdb.client.test;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Proves that a pool-managed sender's startup recovery reaches the user's
 * {@link io.questdb.client.SenderErrorHandler}, registered through
 * {@link QuestDB#builder()}'s {@code errorHandler}, for the two events the
 * recovery delegate can genuinely attribute to the user's own data: a
 * build()-time quarantine ({@link SenderError.Category#DATA_LOSS}) and a real
 * server NACK of the recovered rows (a wire status byte). It also proves the
 * filter's other half: the recovery delegate's own environment noise --
 * connection attempts and never-connected auth/durable-ack TERMINALs against
 * an unreachable or misconfigured server -- stays silent, and that a blocking
 * user handler can never stall {@link QuestDB#close()}.
 */
public class SenderPoolDataLossNotificationTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-pool-data-loss-notification-" + System.nanoTime()).toString();
        Assert.assertEquals("mkdir sf_dir", 0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test(timeout = 60_000L)
    public void testPoolRecoveryQuarantineReachesUserErrorHandler() throws Exception {
        // The C2 regression test: before the fix, recovery builds skipped
        // applyUserCallbacks, so a quarantine's data-loss notification reached
        // nobody. This test FAILS on the pre-fix code.
        TestUtils.assertMemoryLeak(() -> {
            writeTornPoolSlot();
            List<SenderError> received = Collections.synchronizedList(new ArrayList<SenderError>());
            try (TestWebSocketServer good = new TestWebSocketServer(new AckAllHandler())) {
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                try (QuestDB ignored = QuestDB.builder()
                        .fromConfig(poolConfig(good.getPort()))
                        .errorHandler(received::add)
                        .build()) {
                    awaitTrue(20_000, () -> !received.isEmpty(),
                            "pool recovery quarantine must reach the user's errorHandler");
                }
            }
            Assert.assertEquals("exactly one notification -- nothing else may leak through: "
                    + received, 1, received.size());
            SenderError err = received.get(0);
            Assert.assertEquals(SenderError.Category.DATA_LOSS, err.getCategory());
            Assert.assertEquals(SenderError.Policy.ABANDONED, err.getAppliedPolicy());
            Assert.assertNotNull(err.getQuarantinedPath());
            Assert.assertTrue("must name the pool slot [path=" + err.getQuarantinedPath() + ']',
                    err.getQuarantinedPath().contains("pool-0.unreplayable-"));
            Assert.assertTrue("quarantine dir must exist on disk",
                    java.nio.file.Files.isDirectory(Paths.get(sfDir, "pool-0.unreplayable-0")));
        });
    }

    @Test(timeout = 60_000L)
    public void testRecoveryDelegateEnvironmentNoiseStaysSuppressed() throws Exception {
        // The other half of the contract: a recovery delegate retrying against
        // an environmental 401 wall (~1 attempt/second, classified TERMINAL
        // because the delegate has never connected) must NOT reach the
        // handler. Those events carry NO_STATUS_BYTE, which is what the
        // provenance filter keys on.
        TestUtils.assertMemoryLeak(() -> {
            seedHealthyStrandedSlot("pool-0");
            List<SenderError> received = Collections.synchronizedList(new ArrayList<SenderError>());
            try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
                server.setRejectWithStatus(401, "Unauthorized");
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (QuestDB ignored = QuestDB.builder()
                        .fromConfig(poolConfig(server.getPort()))
                        .errorHandler(received::add)
                        .build()) {
                    // Spans at least one full recovery attempt against the 401
                    // wall (recovery retries roughly once a second).
                    Thread.sleep(3_000);
                }
            }
            Assert.assertTrue("errorHandler must stay silent through an environmental 401 wall; got "
                    + received, received.isEmpty());
        });
    }

    @Test(timeout = 60_000L)
    public void testServerNackOfRecoveredRowsReachesUserHandler() throws Exception {
        // Guards the filter's second clause: a real server NACK (carrying its
        // wire status byte) of the user's own recovered rows must pass. A later
        // "simplification" of the filter to DATA_LOSS-only breaks this test.
        TestUtils.assertMemoryLeak(() -> {
            seedHealthyStrandedSlot("pool-0");
            List<SenderError> received = Collections.synchronizedList(new ArrayList<SenderError>());
            try (TestWebSocketServer server = new TestWebSocketServer(new NackAllDataHandler())) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (QuestDB ignored = QuestDB.builder()
                        .fromConfig(poolConfig(server.getPort()))
                        .errorHandler(received::add)
                        .build()) {
                    awaitTrue(20_000, () -> !received.isEmpty(),
                            "a real server NACK of the recovered rows must pass the filter");
                }
            }
            Assert.assertFalse(received.isEmpty());
            Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR & 0xFF,
                    received.get(0).getServerStatusByte());
        });
    }

    @Test(timeout = 60_000L)
    public void testBlockingHandlerCannotStallPoolClose() throws Exception {
        // Justifies the dispatcher over an inline call: the recovery driver /
        // housekeeper thread must never wait on user code, and close() must
        // return within its stop budget even when the handler is parked.
        TestUtils.assertMemoryLeak(() -> {
            writeTornPoolSlot();
            CountDownLatch handlerEntered = new CountDownLatch(1);
            CountDownLatch releaseHandler = new CountDownLatch(1);
            try (TestWebSocketServer good = new TestWebSocketServer(new AckAllHandler())) {
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                long closeMillis;
                QuestDB db = QuestDB.builder()
                        .fromConfig(poolConfig(good.getPort()))
                        .errorHandler(e -> {
                            handlerEntered.countDown();
                            try {
                                releaseHandler.await();
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        })
                        .build();
                try {
                    Assert.assertTrue("quarantine notification must start delivering",
                            handlerEntered.await(20, TimeUnit.SECONDS));
                    long t0 = System.currentTimeMillis();
                    db.close();
                    closeMillis = System.currentTimeMillis() - t0;
                } finally {
                    releaseHandler.countDown();
                }
                Assert.assertTrue("close() must not wait on a parked user handler (took "
                        + closeMillis + "ms)", closeMillis < 10_000L);
            }
        });
    }

    // sender_pool_min=0 is load-bearing: a prewarm build would adopt the torn
    // slot on the NON-recovery path (with the user's full callbacks applied)
    // and the test would no longer exercise recovery delivery at all.
    private String poolConfig(int port) {
        return "ws::addr=localhost:" + port
                + ";sf_dir=" + sfDir
                + ";sender_id=pool"
                + ";sender_pool_min=0;sender_pool_max=1"
                + ";query_pool_min=0;query_pool_max=1"
                + ";initial_connect_retry=async"
                + ";reconnect_initial_backoff_millis=25"
                + ";reconnect_max_backoff_millis=200"
                + ";close_flush_timeout_millis=0;";
    }

    // The SegmentSkipQuarantineTest recipe, retargeted at the pool's slot 0
    // (<sfDir>/pool-0): multiple 512-byte segments, then the oldest segment's
    // FILE_MAGIC overwritten, so the POOL's recovery build of slot 0 hits the
    // constructor-arm quarantine.
    private void writeTornPoolSlot() throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String pad = TestUtils.repeat("x", 64);
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sender_id=pool-0"
                    + ";sf_max_segment_bytes=512"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg)) {
                for (int i = 0; i < 20; i++) {
                    s1.table("foo").stringColumn("p", pad).longColumn("v", i).atNow();
                    s1.flush();
                }
            }
        }
        java.nio.file.Path oldest = Paths.get(sfDir, "pool-0", "sf-initial.sfa");
        Assert.assertTrue("setup: sf-initial.sfa must survive -- nothing acked it",
                java.nio.file.Files.exists(oldest));
        corruptMagic(oldest.toString());
    }

    private static void corruptMagic(String path) {
        int fd = Files.openRW(path);
        Assert.assertTrue("openRW failed", fd >= 0);
        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(buf, 0xBADBAD00);
            Files.write(fd, buf, 4, 0);
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            Files.close(fd);
        }
    }

    // Unacked-but-healthy frames, the shape a crashed run leaves behind
    // (QuestDBFacadeDrainerListenerTest.seedOrphanSlot recipe, retargeted).
    private void seedHealthyStrandedSlot(String slotName) {
        String slotPath = sfDir + "/" + slotName;
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, 4096)) {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, payload[i]);
                }
                for (int i = 0; i < 3; i++) {
                    engine.appendBlocking(buf, 16);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) rmDirRec(child);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static void awaitTrue(long timeoutMillis, BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.assertTrue(message + " (timed out after " + timeoutMillis + "ms)",
                condition.getAsBoolean());
    }

    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class NackAllDataHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildNack(nextSeq++, WebSocketResponse.STATUS_PARSE_ERROR));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks: frames stay unacked so the slot survives close()
        }
    }
}
