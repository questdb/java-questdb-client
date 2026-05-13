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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class QwpQueryClientMultiHostFailoverTest {

    @Test(timeout = 10_000)
    public void testReplicaThen401_FailsFastWithAuthOnSecond() throws Exception {
        try (FakeStatusServer replica = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA");
             FakeStatusServer auth = new FakeStatusServer(401, null)) {
            replica.start();
            auth.start();

            String cfg = "ws::addr=127.0.0.1:" + replica.port() + ",127.0.0.1:" + auth.port()
                    + ";auth_timeout_ms=2000;failover=off;target=any;";
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg)) {
                try {
                    client.connect();
                    Assert.fail("expected connect to throw QwpAuthFailedException");
                } catch (QwpAuthFailedException ae) {
                    Assert.assertEquals(401, ae.getStatusCode());
                    Assert.assertTrue("first endpoint should have been probed",
                            replica.connections.get() >= 1);
                    Assert.assertTrue("auth endpoint must have been probed before short-circuit",
                            auth.connections.get() >= 1);
                }
            }
        }
    }

    @Test(timeout = 10_000)
    public void testAllReplica_FailsWithRoleSummary() throws Exception {
        try (FakeStatusServer r1 = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA");
             FakeStatusServer r2 = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA")) {
            r1.start();
            r2.start();

            String cfg = "ws::addr=127.0.0.1:" + r1.port() + ",127.0.0.1:" + r2.port()
                    + ";auth_timeout_ms=2000;failover=off;target=any;";
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg)) {
                try {
                    client.connect();
                    Assert.fail("expected connect to throw when all endpoints role-reject");
                } catch (HttpClientException ex) {
                    Assert.assertFalse("should not be classified as auth fail",
                            ex instanceof QwpAuthFailedException);
                }
                Assert.assertTrue("both replica endpoints should be probed",
                        r1.connections.get() >= 1 && r2.connections.get() >= 1);
            }
        }
    }

    @Test(timeout = 10_000)
    public void testConnectDoesNotDoubleWalkOnFirstFailure() throws Exception {
        try (FakeStatusServer r1 = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA");
             FakeStatusServer r2 = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA");
             FakeStatusServer r3 = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA")) {
            r1.start();
            r2.start();
            r3.start();

            String cfg = "ws::addr=127.0.0.1:" + r1.port()
                    + ",127.0.0.1:" + r2.port()
                    + ",127.0.0.1:" + r3.port()
                    + ";auth_timeout_ms=2000;failover=off;target=any;";
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg)) {
                try {
                    client.connect();
                    Assert.fail("expected connect to throw when all endpoints role-reject");
                } catch (HttpClientException ignored) {
                }
            }
            Assert.assertEquals("first endpoint must only be probed once on initial connect",
                    1, r1.connections.get());
            Assert.assertEquals("second endpoint must only be probed once on initial connect",
                    1, r2.connections.get());
            Assert.assertEquals("third endpoint must only be probed once on initial connect",
                    1, r3.connections.get());
        }
    }

    private static final class FakeStatusServer implements AutoCloseable {
        final AtomicInteger connections = new AtomicInteger();
        private final String roleHeader;
        private final ServerSocket socket;
        private final int statusCode;
        private volatile boolean running = true;

        FakeStatusServer(int statusCode, String roleHeader) throws IOException {
            this.statusCode = statusCode;
            this.roleHeader = roleHeader;
            this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        }

        int port() {
            return socket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(this::loop, "fake-status-" + statusCode);
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
                    Thread h = new Thread(() -> handle(s), "fake-status-handler-" + statusCode);
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    if (!running) return;
                }
            }
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                connections.incrementAndGet();
                byte[] discard = new byte[8192];
                int n = sock.getInputStream().read(discard);
                if (n < 0) return;
                StringBuilder resp = new StringBuilder();
                resp.append("HTTP/1.1 ").append(statusCode).append(' ').append(reason(statusCode)).append("\r\n");
                if (roleHeader != null) {
                    resp.append(roleHeader).append("\r\n");
                }
                resp.append("Content-Length: 0\r\nConnection: close\r\n\r\n");
                OutputStream out = sock.getOutputStream();
                out.write(resp.toString().getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (Exception ignored) {
            }
        }

        private static String reason(int code) {
            switch (code) {
                case 401:
                    return "Unauthorized";
                case 421:
                    return "Misdirected Request";
                default:
                    return "Status";
            }
        }
    }
}
