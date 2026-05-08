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
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

public class QwpWebSocketSenderMultiEndpointTest {

    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    @Test(timeout = 10_000)
    public void testReplicaThenPrimaryWalksToSecond() throws Exception {
        try (FakeUpgradeServer replica = new FakeUpgradeServer(Mode.REPLICA_REJECT);
             FakeUpgradeServer primary = new FakeUpgradeServer(Mode.UPGRADE_OK)) {
            replica.start();
            primary.start();

            String cfg = "ws::addr=127.0.0.1:" + replica.port() + ",127.0.0.1:" + primary.port()
                    + ";auth_timeout_ms=2000;";
            try (Sender ignored = Sender.fromConfig(cfg)) {
                Assert.assertEquals("primary should accept exactly one upgrade",
                        1, primary.upgradeCount.get());
                Assert.assertTrue("replica should be probed at least once",
                        replica.upgradeCount.get() >= 1);
            }
        }
    }

    @Test(timeout = 10_000)
    public void test401TerminatesAcrossEndpoints() throws Exception {
        try (FakeUpgradeServer auth = new FakeUpgradeServer(Mode.AUTH_401);
             FakeUpgradeServer healthy = new FakeUpgradeServer(Mode.UPGRADE_OK)) {
            auth.start();
            healthy.start();

            String cfg = "ws::addr=127.0.0.1:" + auth.port() + ",127.0.0.1:" + healthy.port()
                    + ";auth_timeout_ms=2000;";
            try {
                Sender.fromConfig(cfg).close();
                Assert.fail("expected auth-fail to terminate connect across endpoints");
            } catch (HttpClientException e) {
                Assert.assertTrue("expected message to mention 401, got: " + e.getMessage(),
                        e.getMessage().contains("401"));
            }
            Assert.assertEquals("healthy peer must NOT be probed when 401 short-circuits",
                    0, healthy.upgradeCount.get());
        }
    }

    @Test(timeout = 10_000)
    public void testAllReplica_AuthFailNotEmitted() throws Exception {
        try (FakeUpgradeServer r1 = new FakeUpgradeServer(Mode.REPLICA_REJECT);
             FakeUpgradeServer r2 = new FakeUpgradeServer(Mode.REPLICA_REJECT)) {
            r1.start();
            r2.start();

            String cfg = "ws::addr=127.0.0.1:" + r1.port() + ",127.0.0.1:" + r2.port()
                    + ";auth_timeout_ms=2000;";
            try {
                Sender.fromConfig(cfg).close();
                Assert.fail("expected connect to fail when all endpoints reject by role");
            } catch (QwpRoleMismatchException e) {
                Assert.assertEquals("PRIMARY", e.getTargetRole());
                Assert.assertTrue("expected REPLICA on observed role: " + e.getMessage(),
                        e.getMessage().contains("REPLICA"));
            }
        }
    }

    private enum Mode {
        UPGRADE_OK,
        REPLICA_REJECT,
        AUTH_401,
    }

    private static final class FakeUpgradeServer implements AutoCloseable {
        final AtomicInteger upgradeCount = new AtomicInteger();
        private final Mode mode;
        private final ServerSocket socket;
        private volatile boolean running = true;

        FakeUpgradeServer(Mode mode) throws IOException {
            this.mode = mode;
            this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        }

        int port() {
            return socket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(this::loop, "fake-upgrade-" + mode);
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void close() throws IOException {
            running = false;
            socket.close();
        }

        private void loop() {
            while (running) {
                try {
                    Socket s = socket.accept();
                    Thread h = new Thread(() -> handle(s), "fake-upgrade-handler-" + mode);
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    if (!running) return;
                }
            }
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        sock.getInputStream(), StandardCharsets.US_ASCII));
                String secKey = null;
                String line;
                while ((line = in.readLine()) != null && !line.isEmpty()) {
                    if (line.regionMatches(true, 0, "Sec-WebSocket-Key:", 0, 18)) {
                        secKey = line.substring(18).trim();
                    }
                }
                upgradeCount.incrementAndGet();
                OutputStream out = sock.getOutputStream();
                switch (mode) {
                    case UPGRADE_OK:
                        out.write(("HTTP/1.1 101 Switching Protocols\r\n"
                                + "Upgrade: websocket\r\n"
                                + "Connection: Upgrade\r\n"
                                + "Sec-WebSocket-Accept: " + computeAcceptKey(secKey) + "\r\n\r\n"
                        ).getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                        // Hold the socket open so the sender can complete its post-upgrade
                        // setup; the test closes the sender which triggers a close handshake.
                        Thread.sleep(500);
                        break;
                    case REPLICA_REJECT:
                        out.write(("HTTP/1.1 421 Misdirected Request\r\n"
                                + "X-QuestDB-Role: REPLICA\r\n"
                                + "Content-Length: 0\r\nConnection: close\r\n\r\n"
                        ).getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                        break;
                    case AUTH_401:
                        out.write(("HTTP/1.1 401 Unauthorized\r\n"
                                + "Content-Length: 0\r\nConnection: close\r\n\r\n"
                        ).getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                        break;
                }
            } catch (Exception ignored) {
            }
        }

        private static String computeAcceptKey(String secKey) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                byte[] digest = sha1.digest((secKey + WEBSOCKET_GUID).getBytes(StandardCharsets.US_ASCII));
                return Base64.getEncoder().encodeToString(digest);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
