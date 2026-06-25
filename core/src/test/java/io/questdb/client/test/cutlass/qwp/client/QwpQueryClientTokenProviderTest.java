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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit coverage for {@link QwpQueryClient#withBearerTokenProvider}: header
 * synthesis, re-query at each resolve (a fresh token per WebSocket upgrade),
 * token validation, null rejection, and mutual exclusion with the fixed-token
 * and basic-auth setters - exercised both through
 * {@link QwpQueryClient#getAuthorizationHeaderForTest()} (which resolves the
 * header the same way a real upgrade does) and, for the real connect path,
 * against a loopback mock that captures the upgrade's {@code Authorization}
 * header and confirms a throwing provider fails the connection attempt. The
 * post-connect guard for the setter lives in
 * {@link QwpQueryClientPostConnectGuardTest}.
 */
public class QwpQueryClientTokenProviderTest {

    @Test
    public void testProviderConflictsWithBasicAuth() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
            try {
                c.withBasicAuth("u", "p");
                Assert.fail("withBasicAuth after withBearerTokenProvider must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }

    @Test
    public void testProviderConflictsWithBearerToken() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
            try {
                c.withBearerToken("other");
                Assert.fail("withBearerToken after withBearerTokenProvider must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }

    @Test
    public void testProviderNullOrBlankReturnRejected() {
        // validateToken rejects a null, empty or blank token RETURNED by the provider before it reaches the
        // "Bearer " header (distinct from testProviderNullRejected, which rejects a null provider at the setter)
        String[] bad = {null, "", "   "};
        for (String token : bad) {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                    .withBearerTokenProvider(() -> token)) {
                try {
                    c.getAuthorizationHeaderForTest();
                    Assert.fail("a null/empty/blank provider token must be rejected, was: [" + token + ']');
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("null or empty"));
                }
            }
        }
    }

    @Test
    public void testProviderNullRejected() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
            try {
                c.withBearerTokenProvider(null);
                Assert.fail("a null provider must be rejected");
            } catch (IllegalArgumentException expected) {
                // expected
            }
        }
    }

    @Test
    public void testProviderQueriedAtEachResolve() {
        int[] counter = {0};
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "tok-" + (counter[0]++))) {
            // each resolve re-queries the provider, so a reconnect presents a fresh token
            Assert.assertEquals("Bearer tok-0", c.getAuthorizationHeaderForTest());
            Assert.assertEquals("Bearer tok-1", c.getAuthorizationHeaderForTest());
        }
    }

    @Test
    public void testProviderSynthesizesBearerHeader() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "abc123")) {
            Assert.assertEquals("Bearer abc123", c.getAuthorizationHeaderForTest());
        }
    }

    @Test(timeout = 15_000)
    public void testProviderTokenSentOnRealUpgrade() throws Exception {
        // drive the REAL connect path (runUpgradeWithTimeout -> resolveAuthorizationHeader), not the test
        // hook: the upgrade request must carry the freshly pulled "Bearer <token>". The mock answers 404
        // (not auth-failed, not terminal) so connect() fails fast after the header was already sent.
        List<String> authHeaders = Collections.synchronizedList(new ArrayList<>());
        ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int port = listener.getLocalPort();
        byte[] respBytes = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
        Thread serverThread = new Thread(() -> {
            while (!listener.isClosed()) {
                try {
                    Socket s = listener.accept();
                    Thread handler = new Thread(() -> {
                        try (Socket sock = s) {
                            byte[] buf = new byte[8192];
                            int n = sock.getInputStream().read(buf);
                            if (n < 0) {
                                return;
                            }
                            String request = new String(buf, 0, n, StandardCharsets.US_ASCII);
                            for (String line : request.split("\r\n")) {
                                if (line.regionMatches(true, 0, "Authorization:", 0, "Authorization:".length())) {
                                    authHeaders.add(line.substring("Authorization:".length()).trim());
                                }
                            }
                            OutputStream os = sock.getOutputStream();
                            os.write(respBytes);
                            os.flush();
                        } catch (Exception ignored) {
                        }
                    }, "qwp-token-upgrade-handler");
                    handler.setDaemon(true);
                    handler.start();
                } catch (Exception ignored) {
                    return;
                }
            }
        }, "qwp-token-upgrade-server");
        serverThread.setDaemon(true);
        serverThread.start();

        try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + port + ";failover=off;target=any;")
                .withBearerTokenProvider(() -> "tok-0")) {
            try {
                client.connect();
                Assert.fail("expected connect to fail on a 404 upgrade");
            } catch (HttpClientException expected) {
                // 404 is neither auth-failed nor terminal: the endpoint is exhausted and connect() fails -
                // but the upgrade request already carried the Bearer header captured above
            }
        } finally {
            listener.close();
            serverThread.join(500);
        }
        Assert.assertEquals("the provider's token must reach the real upgrade request", 1, authHeaders.size());
        Assert.assertEquals("Bearer tok-0", authHeaders.get(0));
    }

    @Test
    public void testProviderTokenValidated() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                .withBearerTokenProvider(() -> "bad\ntoken")) {
            try {
                c.getAuthorizationHeaderForTest();
                Assert.fail("a token carrying a control character must be rejected");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("control or non-ASCII"));
            }
        }
    }

    @Test
    public void testSettingBearerTokenThenProviderConflicts() {
        try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerToken("tok")) {
            try {
                c.withBearerTokenProvider(() -> "other");
                Assert.fail("withBearerTokenProvider after withBearerToken must throw");
            } catch (IllegalStateException expected) {
                // mutually exclusive
            }
        }
    }

    @Test(timeout = 10_000)
    public void testThrowingProviderFailsConnect() throws Exception {
        // a provider that throws must fail the connection attempt on the REAL connect path:
        // resolveAuthorizationHeader runs inside runUpgradeWithTimeout, before the socket connect, so the
        // throw aborts the upgrade; connect() exhausts the single endpoint and surfaces the provider failure
        try (
                ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
                QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + listener.getLocalPort() + ";failover=off;target=any;"
                ).withBearerTokenProvider(() -> {
                    throw new LineSenderException("provider down");
                })
        ) {
            try {
                client.connect();
                Assert.fail("a throwing provider must fail the connection attempt");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("provider down"));
            }
        }
    }
}
