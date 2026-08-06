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
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

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
 * <p>
 * Every test runs under {@code assertMemoryLeak}: a {@link QwpQueryClient}
 * mallocs native scratch in its constructor, so each case proves that scratch
 * is freed on close, including on the connect/error paths.
 */
public class QwpQueryClientTokenProviderTest {

    private static final QwpColumnBatchHandler NOOP_BATCH_HANDLER = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    };

    @Test
    public void testProviderConflictsWithBasicAuth() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
                try {
                    c.withBasicAuth("u", "p");
                    Assert.fail("withBasicAuth after withBearerTokenProvider must throw");
                } catch (IllegalStateException expected) {
                    // mutually exclusive
                }
            }
        });
    }

    @Test
    public void testProviderConflictsWithBearerToken() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerTokenProvider(() -> "tok")) {
                try {
                    c.withBearerToken("other");
                    Assert.fail("withBearerToken after withBearerTokenProvider must throw");
                } catch (IllegalStateException expected) {
                    // mutually exclusive
                }
            }
        });
    }

    @Test
    public void testProviderNullOrBlankReturnRejected() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testProviderNullRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)) {
                try {
                    c.withBearerTokenProvider(null);
                    Assert.fail("a null provider must be rejected");
                } catch (IllegalArgumentException expected) {
                    // expected
                }
            }
        });
    }

    @Test
    public void testProviderQueriedAtEachResolve() throws Exception {
        assertMemoryLeak(() -> {
            int[] counter = {0};
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                    .withBearerTokenProvider(() -> "tok-" + (counter[0]++))) {
                // each resolve re-queries the provider, so a reconnect presents a fresh token
                Assert.assertEquals("Bearer tok-0", c.getAuthorizationHeaderForTest());
                Assert.assertEquals("Bearer tok-1", c.getAuthorizationHeaderForTest());
            }
        });
    }

    @Test
    public void testProviderSynthesizesBearerHeader() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                    .withBearerTokenProvider(() -> "abc123")) {
                Assert.assertEquals("Bearer abc123", c.getAuthorizationHeaderForTest());
            }
        });
    }

    @Test(timeout = 20_000)
    public void testProviderTokenReResolvedOnFailoverReconnect() throws Exception {
        assertMemoryLeak(() -> {
            // The failover reconnect path (reconnectViaTracker) resolves the Authorization header once before its
            // endpoint walk, exactly as connect() does, so a rotating token reaches the reconnect upgrade. This
            // pins that a regression dropping the re-resolve from the reconnect path would be caught: bind endpoint
            // A on the initial connect (capturing tok-0), drop it, then run a query - the failover reconnect to
            // endpoint B must upgrade with a FRESHLY resolved token, not the stale connect-time one.
            AtomicInteger calls = new AtomicInteger();
            TestWebSocketServer a = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            });
            a.setSendServerInfo(true);
            TestWebSocketServer b = new TestWebSocketServer(new ExecDoneQueryServer());
            b.setSendServerInfo(true);
            try {
                a.start();
                b.start();
                Assert.assertTrue(a.awaitStart(5, TimeUnit.SECONDS));
                Assert.assertTrue(b.awaitStart(5, TimeUnit.SECONDS));

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=localhost:" + a.getPort() + ",localhost:" + b.getPort() + ";auth_timeout_ms=2000;")
                        .withBearerTokenProvider(() -> "tok-" + calls.getAndIncrement())) {
                    client.connect();
                    Assert.assertTrue("client must bind the first endpoint on connect", client.isConnected());
                    String aHeader = a.pollAuthorizationHeader(5, TimeUnit.SECONDS);
                    Assert.assertEquals("the initial connect upgrade must carry the first resolved token",
                            "Bearer tok-0", aHeader);

                    // drop endpoint A so the next execute() cannot use its connection and must fail over
                    a.close();

                    // the query fails on the dead A connection, drives the failover loop -> reconnectViaTracker,
                    // which re-resolves the header and upgrades B; B answers EXEC_DONE so execute() returns
                    client.execute("SELECT 1", NOOP_BATCH_HANDLER, false);

                    String bHeader = b.pollAuthorizationHeader(5, TimeUnit.SECONDS);
                    Assert.assertNotNull("the failover reconnect must upgrade endpoint B", bHeader);
                    Assert.assertTrue("the reconnect upgrade must carry a Bearer token, was: " + bHeader,
                            bHeader.startsWith("Bearer tok-"));
                    Assert.assertNotEquals("the failover reconnect must RE-RESOLVE the provider, not reuse the "
                            + "connect-time token", aHeader, bHeader);
                }
            } finally {
                a.close();
                b.close();
            }
        });
    }

    @Test(timeout = 15_000)
    public void testProviderTokenSentOnRealUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            // drive the REAL connect path (connect() -> resolveAuthorizationHeader -> runUpgradeWithTimeout),
            // not the test hook: the upgrade request must carry the freshly pulled "Bearer <token>". The mock
            // answers 404 (not auth-failed, not terminal) so connect() fails fast after the header was sent.
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
        });
    }

    @Test
    public void testProviderTokenValidated() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000)
                    .withBearerTokenProvider(() -> "bad\ntoken")) {
                try {
                    c.getAuthorizationHeaderForTest();
                    Assert.fail("a token carrying a control character must be rejected");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("control or non-ASCII"));
                }
            }
        });
    }

    @Test
    public void testSettingBearerTokenThenProviderConflicts() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpQueryClient c = QwpQueryClient.newPlainText("localhost", 9000).withBearerToken("tok")) {
                try {
                    c.withBearerTokenProvider(() -> "other");
                    Assert.fail("withBearerTokenProvider after withBearerToken must throw");
                } catch (IllegalStateException expected) {
                    // mutually exclusive
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void testThrowingProviderFailsConnect() throws Exception {
        assertMemoryLeak(() -> {
            // a provider that throws must fail the connection attempt on the REAL connect path:
            // resolveAuthorizationHeader runs once before the endpoint walk, so the throw propagates straight
            // out of connect() as the provider's own error (not wrapped as "all endpoints unreachable")
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
                    // the provider's own exception propagates directly (the header is resolved before the
                    // endpoint walk), not wrapped as a transport "all endpoints unreachable" error
                    Assert.assertTrue(expected.getClass().getName(), expected instanceof LineSenderException);
                    Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("provider down"));
                    Assert.assertFalse(expected.getMessage(), expected.getMessage().contains("unreachable"));
                }
            }
        });
    }

    private static byte[] buildExecDone(byte[] queryRequest) {
        int bodyLen = 1 + 8 + 1 + 1; // msg_kind + request_id + op_type + rows_affected varint
        byte[] frame = new byte[QwpConstants.HEADER_SIZE + bodyLen];
        ByteBuffer bb = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 'Q').put((byte) 'W').put((byte) 'P').put((byte) '1');
        bb.put((byte) 1);       // version
        bb.put((byte) 0);       // flags
        bb.putShort((short) 0); // table_count
        bb.putInt(bodyLen);     // payload_length
        bb.put(QwpEgressMsgKind.EXEC_DONE);
        bb.put(queryRequest, 1, 8); // echo request_id verbatim
        bb.put((byte) 0);       // op_type
        bb.put((byte) 0);       // rows_affected = 0
        return frame;
    }

    private static final class ExecDoneQueryServer implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length == 0 || data[0] != QwpEgressMsgKind.QUERY_REQUEST) {
                return;
            }
            try {
                client.sendBinary(buildExecDone(data));
            } catch (IOException e) {
                // best-effort: a failed reply surfaces to the client as a transport error
            }
        }
    }
}
