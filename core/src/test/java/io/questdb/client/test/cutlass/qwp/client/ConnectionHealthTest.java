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

import io.questdb.client.ConnectionHealth;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.client.WebSocketDynamicCredentialTest.ErrorCollector;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance tests C21 (connection health, section 8.4) and C22 (no authentication-outage deadline, section
 * 8.5) of the dynamic-credential specification (design/qwp-token-provider-spec.md).
 */
public class ConnectionHealthTest {
    private static final String SENTINEL = "SENTINEL-TOKEN-7f3a91";

    @Test(timeout = 60_000)
    public void testAuthenticationOutageNeverMakesTheSenderTerminal() throws Exception {
        // C22: an established sender rides out credential-unavailable and 401 rounds, with role rejects in
        // between, for as long as they last. It never becomes terminal, keeps the rows in store-and-forward, and
        // delivers them once the credential is back.
        assertMemoryLeak(() -> {
            AtomicReference<String> providerMode = new AtomicReference<>("ok");
            AtomicReference<Integer> rejectWith = new AtomicReference<>(0);
            ErrorCollector errors = new ErrorCollector();
            WebSocketDynamicCredentialTest.AckHandler handler = new WebSocketDynamicCredentialTest.AckHandler();
            try (TestWebSocketServer server = startServer(handler)) {
                server.setAuthorizationValidator(h -> rejectWith.get());
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(20)
                        .errorHandler(errors)
                        .httpTokenProvider(() -> {
                            if ("fail".equals(providerMode.get())) {
                                throw TokenUnavailableException.retryable("IdP unreachable");
                            }
                            return "TOKEN";
                        })
                        .build()) {
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush();
                    await(() -> handler.frames.get() == 1, 10_000, "the first batch");

                    // the credential becomes unavailable, then the server rejects the token, then every endpoint
                    // role-rejects, then the credential is unavailable again; each server-side rejection is in
                    // place before the previous failure is lifted, so that no upgrade can succeed in between, not
                    // even one whose round pulled a token just before the provider failed again
                    providerMode.set("fail");
                    server.dropAllConnections();
                    await(() -> errors.count("credential-unavailable") >= 1, 10_000, "credential-unavailable rounds");
                    sender.table("t").longColumn("v", 2).atNow();
                    sender.flush(); // kept in store-and-forward while the connection is down
                    Thread.sleep(300);
                    rejectWith.set(401);
                    providerMode.set("ok");
                    await(() -> errors.count("auth-rejected") >= 1, 10_000, "401 rounds");
                    Thread.sleep(300);
                    server.setRejectWithRole("REPLICA");
                    rejectWith.set(0);
                    await(() -> sender.health().getLastFailure().getFailureClass()
                            == ConnectionHealth.FailureClass.ROLE_REJECTED, 10_000, "role-reject rounds");
                    Thread.sleep(300);
                    providerMode.set("fail");
                    await(() -> sender.health().getLastFailure().getFailureClass()
                            == ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE, 10_000, "credential-unavailable again");
                    Thread.sleep(300);
                    Assert.assertEquals("an established sender must never become terminal on its own",
                            0, errors.terminalCount());
                    Assert.assertEquals(ConnectionHealth.State.RECONNECTING, sender.health().getState());

                    // the credential and the primary are back: the buffered row is delivered
                    server.setRejectWithRole(null);
                    providerMode.set("ok");
                    await(() -> handler.frames.get() >= 2, 10_000, "the buffered row");
                    await(() -> sender.health().getState() == ConnectionHealth.State.CONNECTED, 10_000, "recovered");
                    Assert.assertEquals(0, errors.terminalCount());
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testFacadeAggregatesEveryPooledConnection() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer(new ExecDoneHandler())) {
                server.setSendServerInfo(true);
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sender_pool_min=2;query_pool_min=1;";
                try (QuestDB db = QuestDB.connect(cfg)) {
                    ConnectionHealth.Aggregate health = db.health();
                    Assert.assertEquals(3, health.total());
                    Assert.assertEquals(3, health.count(ConnectionHealth.State.CONNECTED));
                    Assert.assertEquals(ConnectionHealth.NONE, health.getOldestOutageSinceEpochMillis());
                    Assert.assertNull(health.getLastFailure());
                    try (Sender s = db.borrowSender()) {
                        Assert.assertEquals(ConnectionHealth.State.CONNECTED, s.health().getState());
                    }
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testQueryClientHealthFollowsConnectFailoverAndRecovery() throws Exception {
        // C21 for egress: connecting, connected, reconnecting after a failed failover - with the class of the
        // failure - and connected again on the next operation; closed at the end.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer a = startServer(new ExecDoneHandler());
                 TestWebSocketServer b = startServer(new ExecDoneHandler())) {
                a.setSendServerInfo(true);
                b.setSendServerInfo(true);
                QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + a.getPort()
                                + ",localhost:" + b.getPort() + ";failover_backoff_initial_ms=0;")
                        .withBearerTokenProvider(() -> SENTINEL);
                try {
                    ConnectionHealth h = client.health();
                    Assert.assertEquals(ConnectionHealth.State.CONNECTING, h.getState());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());

                    client.connect();
                    h = client.health();
                    Assert.assertEquals(ConnectionHealth.State.CONNECTED, h.getState());
                    Assert.assertEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getLastConnectedAtEpochMillis());

                    b.setAuthorizationValidator(x -> 403);
                    a.close();
                    client.execute("SELECT 1", new NoopHandler());
                    h = client.health();
                    Assert.assertEquals(ConnectionHealth.State.RECONNECTING, h.getState());
                    Assert.assertEquals(1, h.getFailedRounds());
                    Assert.assertEquals(ConnectionHealth.FailureClass.AUTH_REJECTED, h.getLastFailure().getFailureClass());
                    Assert.assertEquals(403, h.getLastFailure().getStatusCode());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    Assert.assertFalse(h.toString().contains(SENTINEL));

                    b.setAuthorizationValidator(null);
                    client.execute("SELECT 1", new NoopHandler());
                    h = client.health();
                    Assert.assertEquals(ConnectionHealth.State.CONNECTED, h.getState());
                    Assert.assertEquals(0, h.getFailedRounds());
                    Assert.assertEquals("last_failure is kept after recovery",
                            ConnectionHealth.FailureClass.AUTH_REJECTED, h.getLastFailure().getFailureClass());
                } finally {
                    client.close();
                }
                Assert.assertEquals(ConnectionHealth.State.CLOSED, client.health().getState());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testQueryClientHealthIsFailedWhenItsOnlyAttemptLosesItsConnection() throws Exception {
        // C21 for egress: with failover_max_attempts=1 a query that loses its connection fails, and every later
        // query fails on the same latched failure without reconnecting - the client is done, as with failover off.
        assertMemoryLeak(() -> {
            DropQueryHandler handler = new DropQueryHandler();
            try (TestWebSocketServer server = startServer(handler)) {
                server.setSendServerInfo(true);
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";failover_max_attempts=1;")) {
                    client.connect();
                    handler.drop.set(true);
                    RecordingHandler failed = new RecordingHandler();
                    client.execute("SELECT 1", failed);
                    Assert.assertNotNull("the query must fail", failed.error);
                    Assert.assertTrue(failed.error, failed.error.startsWith("transport failure after 1 execute attempt"));
                    ConnectionHealth h = client.health();
                    Assert.assertEquals(h.toString(), ConnectionHealth.State.FAILED, h.getState());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());

                    // FAILED must hold: the server answers again, yet the next query fails without reconnecting
                    handler.drop.set(false);
                    int upgrades = server.upgradeRequestCount();
                    RecordingHandler next = new RecordingHandler();
                    client.execute("SELECT 1", next);
                    Assert.assertNotNull("the next query must fail", next.error);
                    Assert.assertTrue(next.error, next.error.startsWith("transport failure after 1 execute attempt"));
                    Assert.assertEquals("no reconnect", upgrades, server.upgradeRequestCount());
                    Assert.assertEquals(ConnectionHealth.State.FAILED, client.health().getState());
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testQueryClientHealthReportsAConnectionLostOnTheLastAttempt() throws Exception {
        // C21 for egress: execute() gives up when a query loses its connection on the last attempt its failover
        // budget allows - here attempt 2 of 2, right after a successful failover reconnect. The health must not
        // keep reporting that dead connection as connected; the next operation reconnects.
        assertMemoryLeak(() -> {
            DropQueryHandler handler = new DropQueryHandler();
            try (TestWebSocketServer server = startServer(handler)) {
                server.setSendServerInfo(true);
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";failover_max_attempts=2;failover_backoff_initial_ms=0;")) {
                    client.connect();
                    Assert.assertEquals(ConnectionHealth.State.CONNECTED, client.health().getState());

                    handler.drop.set(true);
                    RecordingHandler failed = new RecordingHandler();
                    client.execute("SELECT 1", failed);
                    Assert.assertNotNull("the query must fail", failed.error);
                    Assert.assertTrue(failed.error, failed.error.startsWith("transport failure after 2 execute attempts"));
                    ConnectionHealth h = client.health();
                    Assert.assertEquals(h.toString(), ConnectionHealth.State.RECONNECTING, h.getState());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());

                    handler.drop.set(false);
                    RecordingHandler recovered = new RecordingHandler();
                    client.execute("SELECT 1", recovered);
                    Assert.assertNull(recovered.error, recovered.error);
                    Assert.assertEquals(1, recovered.execDone);
                    h = client.health();
                    Assert.assertEquals(h.toString(), ConnectionHealth.State.CONNECTED, h.getState());
                    Assert.assertEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testSenderHealthMovesThroughEveryState() throws Exception {
        // C21: connecting -> connected -> reconnecting (with 401 and credential-unavailable failures) ->
        // connected. outage_since and failed_rounds reset on recovery; last_failure is kept; no credential appears.
        assertMemoryLeak(() -> {
            AtomicReference<String> providerMode = new AtomicReference<>("fail");
            AtomicReference<Integer> rejectWith = new AtomicReference<>(0);
            WebSocketDynamicCredentialTest.AckHandler handler = new WebSocketDynamicCredentialTest.AckHandler();
            try (TestWebSocketServer server = startServer(handler)) {
                server.setAuthorizationValidator(h -> rejectWith.get());
                long created = System.currentTimeMillis();
                Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .initialConnectMode(Sender.InitialConnectMode.ASYNC)
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(20)
                        .httpTokenProvider(() -> {
                            if ("fail".equals(providerMode.get())) {
                                throw TokenUnavailableException.retryable("IdP unreachable");
                            }
                            return SENTINEL;
                        })
                        .build();
                try {
                    // connecting, with credential-unavailable rounds
                    await(() -> sender.health().getFailedRounds() >= 2, 10_000, "failed rounds while connecting");
                    ConnectionHealth h = sender.health();
                    Assert.assertEquals(ConnectionHealth.State.CONNECTING, h.getState());
                    Assert.assertTrue(h.getOutageSinceEpochMillis() >= created - 1_000);
                    Assert.assertEquals(ConnectionHealth.NONE, h.getLastConnectedAtEpochMillis());
                    Assert.assertEquals(ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE,
                            h.getLastFailure().getFailureClass());
                    Assert.assertTrue(h.getLastFailure().getMessage(), h.getLastFailure().getMessage().contains("IdP unreachable"));

                    // connected
                    providerMode.set("ok");
                    await(() -> sender.health().getState() == ConnectionHealth.State.CONNECTED, 10_000, "connected");
                    h = sender.health();
                    Assert.assertEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    Assert.assertEquals(0, h.getFailedRounds());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getLastConnectedAtEpochMillis());
                    Assert.assertEquals("last_failure is kept after recovery",
                            ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE, h.getLastFailure().getFailureClass());

                    // reconnecting, rejected with 401
                    rejectWith.set(401);
                    server.dropAllConnections();
                    await(() -> sender.health().getState() == ConnectionHealth.State.RECONNECTING
                            && sender.health().getLastFailure().getFailureClass() == ConnectionHealth.FailureClass.AUTH_REJECTED,
                            10_000, "reconnecting with 401");
                    h = sender.health();
                    Assert.assertEquals(401, h.getLastFailure().getStatusCode());
                    Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    long outageSince = h.getOutageSinceEpochMillis();

                    // still reconnecting, now the provider fails
                    providerMode.set("fail");
                    await(() -> sender.health().getLastFailure().getFailureClass()
                            == ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE, 10_000, "credential-unavailable");
                    h = sender.health();
                    Assert.assertEquals(ConnectionHealth.State.RECONNECTING, h.getState());
                    Assert.assertEquals("the outage did not restart", outageSince, h.getOutageSinceEpochMillis());
                    Assert.assertTrue(h.getFailedRounds() >= 2);
                    Assert.assertFalse("no credential may appear: " + h, h.toString().contains(SENTINEL));

                    // back to connected
                    providerMode.set("ok");
                    rejectWith.set(0);
                    await(() -> sender.health().getState() == ConnectionHealth.State.CONNECTED, 10_000, "recovered");
                    h = sender.health();
                    Assert.assertEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    Assert.assertEquals(0, h.getFailedRounds());
                    Assert.assertEquals(ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE, h.getLastFailure().getFailureClass());
                    Assert.assertFalse(h.toString().contains(SENTINEL));
                } finally {
                    sender.close();
                }
                Assert.assertEquals(ConnectionHealth.State.CLOSED, sender.health().getState());
            }
        });
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler)
            throws IOException, InterruptedException {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    // Answers like ExecDoneHandler but, while drop is set, drops the connection a query arrives on.
    private static final class DropQueryHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicBoolean drop = new AtomicBoolean();
        private final ExecDoneHandler answer = new ExecDoneHandler();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (drop.get() && data.length > 0 && data[0] == QwpEgressMsgKind.QUERY_REQUEST) {
                // off this read thread: ClientHandler.close() joins it
                Thread closer = new Thread(client::close, "drop-query-connection");
                closer.setDaemon(true);
                closer.start();
            } else {
                answer.onBinaryMessage(client, data);
            }
        }
    }

    private static final class ExecDoneHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final WebSocketDynamicCredentialTest.AckHandler ack = new WebSocketDynamicCredentialTest.AckHandler();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length > 0 && data[0] == QwpEgressMsgKind.QUERY_REQUEST) {
                int bodyLen = 1 + 8 + 1 + 1;
                byte[] frame = new byte[QwpConstants.HEADER_SIZE + bodyLen];
                ByteBuffer bb = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
                bb.put((byte) 'Q').put((byte) 'W').put((byte) 'P').put((byte) '1');
                bb.put((byte) 1).put((byte) 0).putShort((short) 0).putInt(bodyLen);
                bb.put(QwpEgressMsgKind.EXEC_DONE);
                bb.put(data, 1, 8);
                bb.put((byte) 0).put((byte) 0);
                try {
                    client.sendBinary(frame);
                } catch (IOException ignored) {
                    // surfaces to the client as a transport error
                }
            } else {
                ack.onBinaryMessage(client, data);
            }
        }
    }

    private static final class NoopHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    }

    private static final class RecordingHandler implements QwpColumnBatchHandler {
        String error;
        int execDone;

        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            error = message;
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            execDone++;
        }
    }
}
