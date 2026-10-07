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

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpCredentialUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.auth.TokenTestKit.ScriptedSource;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The dynamic-credential specification (design/qwp-token-provider-spec.md) on the QWP egress client: the one
 * retry after a 401 on connect and on failover reconnect (section 8.2, C10/C11/C23), failure-class naming in
 * the reported error, and egress recovery - a client whose failover reconnect failed reconnects on its next
 * operation rather than staying unusable (section 8.3).
 */
public class QwpQueryClientDynamicCredentialTest {
    private static final long HOUR = 3_600_000L;

    @Test(timeout = 30_000)
    public void testConnectCredentialUnavailableNamesTheClassAndContactsNoEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer();
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort() + ";")
                         .withBearerTokenProvider(() -> {
                             throw TokenUnavailableException.retryable("IMDS unreachable");
                         })) {
                try {
                    client.connect();
                    Assert.fail();
                } catch (QwpCredentialUnavailableException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("credential-unavailable: "));
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("IMDS unreachable"));
                    Assert.assertTrue(e.isRetryable());
                    Assert.assertTrue(e.getCause() instanceof TokenUnavailableException);
                }
                Assert.assertEquals("no endpoint may be contacted", 0, server.upgradeRequestCount());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConnectInsufficientScopeIsNotRetried() throws Exception {
        // C23 on egress: a Bearer challenge with an error other than invalid_token gets no retry.
        assertMemoryLeak(() -> {
            ScriptedSource source = twoTokens();
            try (TestWebSocketServer server = startServer();
                 RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build();
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort() + ";")
                         .withBearerTokenProvider(provider)) {
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 401 : 0);
                server.setRejectWwwAuthenticate("Bearer error=\"insufficient_scope\"");
                try {
                    client.connect();
                    Assert.fail();
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals("insufficient_scope", e.getBearerError());
                    Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("auth-rejected"));
                }
                Assert.assertEquals(1, server.upgradeRequestCount());
                Assert.assertEquals(1, source.calls());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConnectPersistent401IsRetriedOnceThenFails() throws Exception {
        // C11 on egress: the failure survives the one retry and fails the operation.
        assertMemoryLeak(() -> {
            ScriptedSource source = twoTokens();
            try (TestWebSocketServer server = startServer();
                 RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build();
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort() + ";")
                         .withBearerTokenProvider(provider)) {
                server.setAuthorizationValidator(h -> 401);
                try {
                    client.connect();
                    Assert.fail();
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(401, e.getStatusCode());
                }
                Assert.assertEquals(Arrays.asList("Bearer T1", "Bearer T2"), headers(server));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConnectStaleTokenIsRetriedOnceWithTheRefreshedToken() throws Exception {
        // C10 on egress: one 401 for the stale token, then the same endpoint with the refreshed one.
        assertMemoryLeak(() -> {
            ScriptedSource source = twoTokens();
            try (TestWebSocketServer server = startServer();
                 RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build();
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort() + ";")
                         .withBearerTokenProvider(provider)) {
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 401 : 0);
                client.connect();
                Assert.assertTrue(client.isConnected());
                Assert.assertEquals(Arrays.asList("Bearer T1", "Bearer T2"), headers(server));
                Assert.assertEquals(1, server.authRejectCount());
                ResultCollector result = new ResultCollector();
                client.execute("SELECT 1", result);
                Assert.assertNull(result.error.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFailedFailoverReconnectRecoversOnTheNextExecute() throws Exception {
        // Egress recovery (section 8.3): a failover reconnect that fails must not leave the client permanently
        // unusable. Before the fix, every later execute() threw "QwpQueryClient not connected".
        assertMemoryLeak(() -> {
            try (TestWebSocketServer a = startServer();
                 TestWebSocketServer b = startServer();
                 QwpQueryClient client = QwpQueryClient.fromConfig(
                                 "ws::addr=localhost:" + a.getPort() + ",localhost:" + b.getPort()
                                         + ";failover_backoff_initial_ms=0;")
                         .withBearerTokenProvider(() -> "TOKEN")) {
                client.connect();
                ResultCollector first = new ResultCollector();
                client.execute("SELECT 1", first);
                Assert.assertNull(first.error.get());

                // A dies; B rejects the credential: the failover reconnect fails, naming the class
                b.setAuthorizationValidator(h -> 401);
                a.close();
                ResultCollector failed = new ResultCollector();
                client.execute("SELECT 1", failed);
                Assert.assertNotNull("the operation must fail", failed.error.get());
                Assert.assertTrue(failed.error.get(), failed.error.get().contains("auth-rejected"));
                Assert.assertFalse(client.isConnected());

                // B accepts again: the next operation reconnects instead of failing forever
                b.setAuthorizationValidator(null);
                ResultCollector recovered = new ResultCollector();
                client.execute("SELECT 1", recovered);
                Assert.assertNull("the next execute() must reconnect: " + recovered.error.get(), recovered.error.get());
                Assert.assertTrue(recovered.done.get() > 0);
                Assert.assertTrue(client.isConnected());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFailoverCredentialUnavailableNamesTheClass() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger pulls = new AtomicInteger();
            try (TestWebSocketServer a = startServer();
                 TestWebSocketServer b = startServer();
                 QwpQueryClient client = QwpQueryClient.fromConfig(
                                 "ws::addr=localhost:" + a.getPort() + ",localhost:" + b.getPort()
                                         + ";failover_backoff_initial_ms=0;")
                         .withBearerTokenProvider(() -> {
                             if (pulls.incrementAndGet() > 1) {
                                 throw TokenUnavailableException.retryable("IdP unreachable");
                             }
                             return "TOKEN";
                         })) {
                client.connect();
                a.close();
                ResultCollector failed = new ResultCollector();
                client.execute("SELECT 1", failed);
                Assert.assertNotNull(failed.error.get());
                Assert.assertTrue(failed.error.get(), failed.error.get().contains("credential-unavailable: IdP unreachable"));
                Assert.assertEquals("no endpoint contacted on the failed failover", 0, b.upgradeRequestCount());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFailoverReconnectRetriesAStaleTokenOnce() throws Exception {
        // C10 on the egress failover path: B rejects the stale token; the failover reconnect refreshes and
        // retries B once, and the query completes.
        assertMemoryLeak(() -> {
            ScriptedSource source = twoTokens();
            try (TestWebSocketServer a = startServer();
                 TestWebSocketServer b = startServer();
                 RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build();
                 QwpQueryClient client = QwpQueryClient.fromConfig(
                                 "ws::addr=localhost:" + a.getPort() + ",localhost:" + b.getPort()
                                         + ";failover_backoff_initial_ms=0;")
                         .withBearerTokenProvider(provider)) {
                b.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 401 : 0);
                client.connect();
                Assert.assertEquals("Bearer T1", a.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                a.close();
                ResultCollector result = new ResultCollector();
                client.execute("SELECT 1", result);
                Assert.assertNull(result.error.get());
                Assert.assertEquals(Arrays.asList("Bearer T1", "Bearer T2"), headers(b));
            }
        });
    }

    @Test(timeout = 60_000)
    public void testPooledQueryClientRecoversAfterAFailedFailover() throws Exception {
        // The suspected dead-pooled-worker bug (design/entra-id-qwp-auth.md, section 3): a pooled query client
        // whose failover reconnect failed went back to the pool still disconnected, and with query_pool_min
        // keeping it alive every later borrow failed with "QwpQueryClient not connected".
        assertMemoryLeak(() -> {
            try (TestWebSocketServer a = startServer();
                 TestWebSocketServer b = startServer()) {
                String cfg = "ws::addr=localhost:" + a.getPort() + ",localhost:" + b.getPort()
                        + ";sender_pool_min=0;query_pool_min=1;query_pool_max=1;failover_backoff_initial_ms=0;";
                try (QuestDB db = QuestDB.connect(cfg, () -> "TOKEN")) {
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT 1").handler(new ResultCollector()).submit().await();
                    }
                    b.setAuthorizationValidator(h -> 401);
                    a.close();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT 1").handler(new ResultCollector()).submit().await();
                        Assert.fail("the failover reconnect must fail");
                    } catch (QueryException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("auth-rejected"));
                    }
                    b.setAuthorizationValidator(null);
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT 1").handler(new ResultCollector()).submit().await();
                    } catch (QueryException e) {
                        Assert.fail("the pooled client must reconnect on its next operation: " + e.getMessage());
                    }
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

    private static List<String> headers(TestWebSocketServer server) throws InterruptedException {
        List<String> headers = new ArrayList<>();
        String h;
        while ((h = server.pollAuthorizationHeader(200, TimeUnit.MILLISECONDS)) != null) {
            headers.add(h);
        }
        return headers;
    }

    private static TestWebSocketServer startServer() throws IOException, InterruptedException {
        TestWebSocketServer server = new TestWebSocketServer(new ExecDoneQueryServer());
        server.setSendServerInfo(true);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static ScriptedSource twoTokens() {
        return new ScriptedSource()
                .thenToken("T1", System.currentTimeMillis() + HOUR)
                .thenToken("T2", System.currentTimeMillis() + HOUR);
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

    private static final class ResultCollector implements QwpColumnBatchHandler {
        final AtomicInteger done = new AtomicInteger();
        final AtomicReference<String> error = new AtomicReference<>();

        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
            done.incrementAndGet();
        }

        @Override
        public void onError(byte status, String message) {
            error.set(message);
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            done.incrementAndGet();
        }
    }
}
