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

import io.questdb.client.Sender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Behavior of {@code initial_connect_retry}: when the server is briefly
 * unavailable at startup, the sender should keep trying through the
 * configured cap (instead of failing immediately).
 */
public class InitialConnectRetryTest {

    /**
     * Temp sf_dir for retry-mode tests. The builder does NOT require
     * sf_dir for any initial_connect_retry mode — memory-mode senders
     * share the same retry machinery, buffering rows in the in-RAM
     * cursor ring instead of on disk. These tests use an sf_dir so the
     * retried rows are disk-backed and the tests exercise the durable
     * SF path.
     */
    private static String makeSfDir() {
        return java.nio.file.Paths.get(
                System.getProperty("java.io.tmpdir"),
                "qdb-init-retry-" + System.nanoTime()).toString();
    }

    private static void rmDirRecursive(String path) {
        try {
            java.nio.file.Path p = java.nio.file.Paths.get(path);
            if (!java.nio.file.Files.exists(p)) return;
            java.nio.file.Files.walk(p)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(f -> {
                        try {
                            java.nio.file.Files.deleteIfExists(f);
                        } catch (IOException ignored) {
                            // best-effort
                        }
                    });
        } catch (IOException ignored) {
            // best-effort
        }
    }

    @Test
    public void testWithRetryGivesUpAfterCap() {
        // No server. With retry on, fromConfig must run the retry loop and
        // ultimately throw with the connectWithRetry-shaped message that
        // names the elapsed budget and attempt count. The actual budget
        // honoring is observable through that message — we don't need a
        // wall-clock check.
        int port = TestPorts.findUnusedPort();
        String sfDir = makeSfDir();
        try {
            String cfg = "ws::addr=127.0.0.1:" + port
                    + ";sf_dir=" + sfDir
                    + ";initial_connect_retry=true"
                    + ";reconnect_max_duration_millis=400"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50;";
            try (Sender ignored = Sender.fromConfig(cfg)) {
                Assert.fail("expected give-up after cap");
            } catch (Exception expected) {
                String msg = expected.getMessage();
                Assert.assertNotNull("error must have a message", msg);
                Assert.assertTrue("error must come from the retry loop: " + msg,
                        msg.contains("initial connect") && msg.contains("attempts"));
            }
        } finally {
            rmDirRecursive(sfDir);
        }
    }

    @Test
    public void testWithRetrySucceedsWhenServerComesUpInTime() throws Exception {
        // initial_connect_retry=true; we open the sender BEFORE starting
        // the server, then start the server in a background thread after
        // a short delay. The retry loop should see the server come up and
        // proceed cleanly.
        AckHandler handler = new AckHandler();
        TestWebSocketServer server = new TestWebSocketServer(handler);
        int port = server.getPort();
        Thread starter = new Thread(() -> {
            try {
                Thread.sleep(300);
                server.start();
            } catch (Exception e) {
                // best-effort
            }
        }, "delayed-server-start");
        starter.setDaemon(true);
        starter.start();
        String sfDir = makeSfDir();
        try {
            String cfg = "ws::addr=127.0.0.1:" + port
                    + ";sf_dir=" + sfDir
                    + ";initial_connect_retry=true"
                    + ";reconnect_max_duration_millis=5000"
                    + ";reconnect_initial_backoff_millis=50"
                    + ";reconnect_max_backoff_millis=200"
                    + ";close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
        } finally {
            try {
                server.close();
            } catch (Exception ignored) {
                // already closed
            }
            rmDirRecursive(sfDir);
        }
    }

    @Test
    public void testWithoutRetryFailsImmediately() {
        // No server on this port. With no reconnect_* knob set and no
        // initial_connect_retry, the resolved mode is OFF, so fromConfig
        // must throw on the first connect failure rather than enter the
        // retry loop. We assert the structural shape of the error: the
        // raw "Failed to connect" message from buildAndConnect, NOT the
        // "initial connect ... attempts" message connectWithRetry produces.
        int port = TestPorts.findUnusedPort();
        // Use the IPv4 literal so the test doesn't pay first-call
        // getaddrinfo("localhost") cost on Windows (1-2 s cold lookup).
        try (Sender ignored = Sender.fromConfig("ws::addr=127.0.0.1:" + port + ";")) {
            Assert.fail("expected immediate connect failure");
        } catch (Exception expected) {
            String msg = expected.getMessage();
            Assert.assertNotNull("error must have a message", msg);
            Assert.assertTrue("error must be the raw connect-refused: " + msg,
                    msg.contains("Failed to connect"));
            Assert.assertFalse("error must NOT mention the retry loop: " + msg,
                    msg.contains("attempts"));
        }
    }

    @Test
    public void testReconnectKnobImplicitlyPromotesInitialConnectToSync() {
        // No initial_connect_retry on the conf string, but the user did set
        // reconnect_max_duration_millis. The resolution rule on InitialConnectMode
        // promotes that to SYNC so the budget the user wrote actually applies
        // to the first connect. We prove SYNC ran by asserting the error message
        // is the connectWithRetry-shaped one ("initial connect ... attempts"),
        // not the raw single-attempt "Failed to connect" surface.
        int port = TestPorts.findUnusedPort();
        String sfDir = makeSfDir();
        try {
            String cfg = "ws::addr=127.0.0.1:" + port
                    + ";sf_dir=" + sfDir
                    + ";reconnect_max_duration_millis=400"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50;";
            try (Sender ignored = Sender.fromConfig(cfg)) {
                Assert.fail("expected give-up after implicit-SYNC cap");
            } catch (Exception expected) {
                String msg = expected.getMessage();
                Assert.assertNotNull("error must have a message", msg);
                Assert.assertTrue("implicit SYNC must drive the retry loop: " + msg,
                        msg.contains("initial connect") && msg.contains("attempts"));
            }
        } finally {
            rmDirRecursive(sfDir);
        }
    }

    @Test
    public void testExplicitOffSuppressesImplicitSync() {
        // Tuning a reconnect_* knob would normally promote initial connect to
        // SYNC, but an explicit initial_connect_retry=off opts back out. The
        // failure surface here must be the raw single-attempt "Failed to
        // connect", proving the explicit OFF won over the implicit upgrade.
        int port = TestPorts.findUnusedPort();
        String cfg = "ws::addr=127.0.0.1:" + port
                + ";reconnect_max_duration_millis=400"
                + ";initial_connect_retry=off;";
        try (Sender ignored = Sender.fromConfig(cfg)) {
            Assert.fail("expected immediate connect failure when initial_connect_retry=off");
        } catch (Exception expected) {
            String msg = expected.getMessage();
            Assert.assertNotNull("error must have a message", msg);
            Assert.assertTrue("explicit off must yield raw connect failure: " + msg,
                    msg.contains("Failed to connect"));
            Assert.assertFalse("explicit off must suppress the retry loop: " + msg,
                    msg.contains("attempts"));
        }
    }

    /**
     * Acks every binary frame so the sender's flush completes.
     */
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
