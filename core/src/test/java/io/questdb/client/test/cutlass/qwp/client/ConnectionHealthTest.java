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
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.client.WebSocketDynamicCredentialTest.ErrorCollector;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance tests C21 (connection health, section 8.4) and C22 (no authentication-outage deadline, section
 * 8.5) of the dynamic-credential specification (design/qwp-token-provider-spec.md).
 */
public class ConnectionHealthTest {
    private static final String SENTINEL = "SENTINEL-TOKEN-7f3a91";

    @Test
    public void testAggregateReportsTheOldestOutageAndTheLatestFailure() {
        // C21 for the facade: QuestDB.health() reports how long the longest outage has lasted and what failed last,
        // whatever the order of the pooled connections.
        ConnectionHealth.Failure olderFailure = new ConnectionHealth.Failure(
                ConnectionHealth.FailureClass.TRANSPORT, 0, "connection refused", 6_000L);
        ConnectionHealth.Failure newerFailure = new ConnectionHealth.Failure(
                ConnectionHealth.FailureClass.AUTH_REJECTED, 401, "auth-rejected", 10_000L);
        List<ConnectionHealth> healths = Arrays.asList(
                new ConnectionHealth(ConnectionHealth.State.CONNECTED, 1_000L, ConnectionHealth.NONE, 0, null),
                new ConnectionHealth(ConnectionHealth.State.RECONNECTING, 1_000L, 9_000L, 1, newerFailure),
                new ConnectionHealth(ConnectionHealth.State.RECONNECTING, 1_000L, 5_000L, 3, olderFailure),
                new ConnectionHealth(ConnectionHealth.State.CONNECTING, ConnectionHealth.NONE, 7_000L, 0, null)
        );
        for (int rotation = 0; rotation < healths.size(); rotation++) {
            List<ConnectionHealth> ordered = new ArrayList<>(healths);
            Collections.rotate(ordered, rotation);
            ConnectionHealth.Aggregate aggregate = ConnectionHealth.Aggregate.of(ordered);
            Assert.assertEquals(4, aggregate.total());
            Assert.assertEquals(1, aggregate.count(ConnectionHealth.State.CONNECTED));
            Assert.assertEquals(2, aggregate.count(ConnectionHealth.State.RECONNECTING));
            Assert.assertEquals(1, aggregate.count(ConnectionHealth.State.CONNECTING));
            Assert.assertEquals("the oldest outage [rotation=" + rotation + ']',
                    5_000L, aggregate.getOldestOutageSinceEpochMillis());
            Assert.assertSame("the most recent failure [rotation=" + rotation + ']',
                    newerFailure, aggregate.getLastFailure());
        }
    }

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
    public void testQueryClientHealthIsFailedWhenFailoverIsOffAndItLosesItsConnection() throws Exception {
        // C21 for egress: with failover off a query that loses its connection fails, and the failure stays latched,
        // so every later query fails on it without reconnecting - the client is done.
        assertMemoryLeak(() -> assertQueryClientFailsForGood("failover=off;", null));
    }

    @Test(timeout = 60_000)
    public void testQueryClientHealthIsFailedWhenItsOnlyAttemptLosesItsConnection() throws Exception {
        // C21 for egress: with failover_max_attempts=1 a query that loses its connection fails, and every later
        // query fails on the same latched failure without reconnecting - the client is done, as with failover off.
        assertMemoryLeak(() -> assertQueryClientFailsForGood("failover_max_attempts=1;",
                "transport failure after 1 execute attempt"));
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
    public void testSenderHealthIsFailedWithAnOutageAfterATerminalRejection() throws Exception {
        // C21: a sender that becomes terminal while connected - the server rejects a batch with a TERMINAL category -
        // reports FAILED, and its outage starts there, since it will never reach the server again.
        assertMemoryLeak(() -> {
            ErrorCollector errors = new ErrorCollector();
            try (TestWebSocketServer server = startServer(new RejectingHandler());
                 Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                         .address("localhost:" + server.getPort())
                         .errorHandler(errors)
                         .build()) {
                ConnectionHealth h = sender.health();
                Assert.assertEquals(ConnectionHealth.State.CONNECTED, h.getState());
                Assert.assertEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());

                sender.table("t").longColumn("v", 1).atNow();
                sender.flush();
                // the I/O thread latches FAILED before it hands the terminal error to the handler
                await(() -> errors.terminalCount() >= 1, 10_000, "the terminal rejection");
                h = sender.health();
                Assert.assertEquals(h.toString(), ConnectionHealth.State.FAILED, h.getState());
                Assert.assertNotEquals("a failed sender is without a connection: " + h,
                        ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                Assert.assertTrue(h.toString(), h.getOutageSinceEpochMillis() >= h.getLastConnectedAtEpochMillis());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testSenderHealthIsReconnectingFromTheMomentItLosesItsConnection() throws Exception {
        // C21: an established sender that loses its connection reports RECONNECTING, with the outage started, while
        // its first reconnect round is still running - not only once a round has failed. That round can be slow: a
        // provider fetching a token, or a connect to a host that does not answer.
        assertMemoryLeak(() -> {
            AtomicBoolean holdPulls = new AtomicBoolean();
            CountDownLatch pullHeld = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (TestWebSocketServer server = startServer(new WebSocketDynamicCredentialTest.AckHandler());
                 Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                         .address("localhost:" + server.getPort())
                         .httpTokenProvider(() -> {
                             if (holdPulls.get()) {
                                 pullHeld.countDown();
                                 try {
                                     release.await(30, TimeUnit.SECONDS);
                                 } catch (InterruptedException e) {
                                     Thread.currentThread().interrupt();
                                 }
                             }
                             return SENTINEL;
                         })
                         .build()) {
                Assert.assertEquals(ConnectionHealth.State.CONNECTED, sender.health().getState());
                try {
                    holdPulls.set(true);
                    server.dropAllConnections();
                    Assert.assertTrue("the reconnect round pulls a credential", pullHeld.await(10, TimeUnit.SECONDS));
                    ConnectionHealth h = sender.health();
                    Assert.assertEquals(h.toString(), ConnectionHealth.State.RECONNECTING, h.getState());
                    Assert.assertNotEquals("the outage starts when the connection is lost: " + h,
                            ConnectionHealth.NONE, h.getOutageSinceEpochMillis());
                    Assert.assertEquals("no round has failed yet", 0, h.getFailedRounds());
                } finally {
                    holdPulls.set(false);
                    release.countDown();
                }
                await(() -> sender.health().getState() == ConnectionHealth.State.CONNECTED, 10_000, "reconnected");
                Assert.assertEquals(ConnectionHealth.NONE, sender.health().getOutageSinceEpochMillis());
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

    // A query client loses its connection in the middle of a query, and its configuration lets it never reconnect:
    // the query fails, and the health is FAILED with an outage. FAILED holds when a later query fails on the same
    // latched failure without contacting the server. errorPrefix, when not null, starts both errors.
    private static void assertQueryClientFailsForGood(String configKeys, String errorPrefix) throws Exception {
        DropQueryHandler handler = new DropQueryHandler();
        try (TestWebSocketServer server = startServer(handler)) {
            server.setSendServerInfo(true);
            try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort() + ';'
                    + configKeys)) {
                client.connect();
                handler.drop.set(true);
                RecordingHandler failed = new RecordingHandler();
                client.execute("SELECT 1", failed);
                Assert.assertNotNull("the query must fail", failed.error);
                Assert.assertTrue(failed.error, errorPrefix == null || failed.error.startsWith(errorPrefix));
                ConnectionHealth h = client.health();
                Assert.assertEquals(h.toString(), ConnectionHealth.State.FAILED, h.getState());
                Assert.assertNotEquals(ConnectionHealth.NONE, h.getOutageSinceEpochMillis());

                // FAILED must hold: the server answers again, yet the next query fails without reconnecting
                handler.drop.set(false);
                int upgrades = server.upgradeRequestCount();
                RecordingHandler next = new RecordingHandler();
                client.execute("SELECT 1", next);
                Assert.assertNotNull("the next query must fail", next.error);
                Assert.assertTrue(next.error, errorPrefix == null || next.error.startsWith(errorPrefix));
                Assert.assertEquals("no reconnect", upgrades, server.upgradeRequestCount());
                Assert.assertEquals(ConnectionHealth.State.FAILED, client.health().getState());
            }
        }
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

    // Rejects every batch with PARSE_ERROR, a TERMINAL category: replaying the same bytes cannot succeed.
    private static final class RejectingHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong sequence = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildNack(sequence.getAndIncrement(), WebSocketResponse.STATUS_PARSE_ERROR));
            } catch (IOException ignored) {
                // surfaces to the client as a transport error
            }
        }
    }
}
