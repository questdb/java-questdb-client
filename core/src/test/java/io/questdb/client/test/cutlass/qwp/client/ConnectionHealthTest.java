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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.client.WebSocketDynamicCredentialTest.ErrorCollector;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance tests C21 (connection health, section 8.4) and C22 (the authentication-outage deadline, section
 * 8.5) of the dynamic-credential specification (design/qwp-token-provider-spec.md).
 */
public class ConnectionHealthTest {
    private static final String SENTINEL = "SENTINEL-TOKEN-7f3a91";

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 60_000)
    public void testDeadlineIsNotResetOrFiredByOtherFailureClasses() throws Exception {
        // C22: rounds that fail for other reasons neither reset the clock nor fire the deadline. A credential
        // outage, then role rejects and a transport outage that together outlast the deadline, then the
        // credential outage again: the deadline fires on the first authentication-class round after them.
        assertMemoryLeak(() -> {
            final long deadlineMillis = 1_000;
            AtomicReference<String> mode = new AtomicReference<>("ok");
            ErrorCollector errors = new ErrorCollector();
            TestWebSocketServer server = startServer(new WebSocketDynamicCredentialTest.DropAfterFirstAckHandler());
            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + server.getPort())
                    .reconnectInitialBackoffMillis(10)
                    .reconnectMaxBackoffMillis(20)
                    .authFailureMaxDurationMillis(deadlineMillis)
                    .errorHandler(errors)
                    .httpTokenProvider(() -> {
                        if ("fail".equals(mode.get())) {
                            throw TokenUnavailableException.retryable("IdP unreachable");
                        }
                        return "TOKEN";
                    })
                    .build()) {
                mode.set("fail");
                sender.table("t").longColumn("v", 1).atNow();
                sender.flush(); // ACKed, then the server drops the connection
                await(() -> errors.count("credential-unavailable") >= 1, 10_000, "the credential outage");
                long authOutageStart = System.nanoTime();
                // Other classes: every endpoint role-rejects (set before the provider recovers, so no upgrade can
                // succeed in between and reset the clock), then the server goes away (transport).
                server.setRejectWithRole("REPLICA");
                mode.set("ok");
                Thread.sleep(deadlineMillis * 2 / 3);
                server.close();
                Thread.sleep(deadlineMillis * 2 / 3);
                Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - authOutageStart) > deadlineMillis);
                Assert.assertEquals("role rejects and transport failures must not fire the deadline",
                        0, errors.terminalCount());
                // the credential outage returns: the clock was never reset, so the first such round fires
                mode.set("fail");
                await(() -> errors.terminalCount() == 1, 10_000, "the deadline to fire");
                SenderError terminal = terminal(errors);
                Assert.assertTrue(terminal.getServerMessage(),
                        terminal.getServerMessage().contains("credential-unavailable persisted for"));
                Assert.assertEquals(ConnectionHealth.State.FAILED, sender.health().getState());
            } catch (LineSenderException expected) {
                // close() rethrows the latched terminal
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("authentication outage deadline"));
            } finally {
                server.close();
            }
        });
    }

    @Test(timeout = 60_000)
    public void testDeadlineMakesTheSenderTerminalAndKeepsTheData() throws Exception {
        // C22: set, the sender becomes terminal on an authentication-class round once the duration has passed;
        // the error names the class and the elapsed time, is reported as terminal and thrown from later producer
        // calls, and the unacknowledged rows stay on disk.
        assertMemoryLeak(() -> {
            String sfDir = temp.newFolder("deadline").getAbsolutePath();
            ErrorCollector errors = new ErrorCollector();
            AtomicReference<Integer> rejectWith = new AtomicReference<>(0);
            try (TestWebSocketServer server = startServer(new WebSocketDynamicCredentialTest.DropAfterFirstAckHandler())) {
                server.setAuthorizationValidator(h -> rejectWith.get());
                Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .storeAndForwardDir(sfDir)
                        .senderId("deadline")
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(20)
                        .closeFlushTimeoutMillis(0)
                        .authFailureMaxDurationMillis(500)
                        .errorHandler(errors)
                        .httpTokenProvider(() -> SENTINEL)
                        .build();
                try {
                    rejectWith.set(401);
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush(); // ACKed, then dropped: every reconnect is now rejected
                    await(() -> server.authRejectCount() >= 1, 10_000, "the first 401");
                    long start = System.nanoTime();
                    sender.table("t").longColumn("v", 2).atNow();
                    sender.flush(); // buffered in store-and-forward
                    await(() -> errors.terminalCount() == 1, 10_000, "the deadline to fire");
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    Assert.assertTrue("must not fire before the deadline, fired at " + elapsedMillis + " ms",
                            elapsedMillis >= 400);
                    SenderError terminal = terminal(errors);
                    Assert.assertEquals(SenderError.Category.SECURITY_ERROR, terminal.getCategory());
                    Assert.assertTrue(terminal.getServerMessage(), terminal.getServerMessage().contains("auth-rejected"));
                    Assert.assertTrue(terminal.getServerMessage(), terminal.getServerMessage().contains("persisted for"));
                    Assert.assertFalse(terminal.getServerMessage().contains(SENTINEL));
                    try {
                        sender.table("t").longColumn("v", 3).atNow();
                        sender.flush();
                        Assert.fail("later producer calls must throw the terminal");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("authentication outage deadline"));
                    }
                    Assert.assertEquals(ConnectionHealth.State.FAILED, sender.health().getState());
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() may rethrow the terminal
                    }
                }
            }
            Assert.assertEquals("the unacknowledged rows stay on disk for a later sender or an orphan drain",
                    1, OrphanScanner.scan(sfDir, "someone-else").size());
        });
    }

    @Test(timeout = 60_000)
    public void testDeadlineIsResetByASuccessfulUpgradeAndUnsetMeansNoDeadline() throws Exception {
        // C22: a successful upgrade resets the clock; and without the key, a 401 outage is retried indefinitely.
        assertMemoryLeak(() -> {
            for (boolean armed : new boolean[]{true, false}) {
                ErrorCollector errors = new ErrorCollector();
                AtomicReference<Integer> rejectWith = new AtomicReference<>(0);
                WebSocketDynamicCredentialTest.AckHandler handler = new WebSocketDynamicCredentialTest.AckHandler();
                try (TestWebSocketServer server = startServer(handler)) {
                    server.setAuthorizationValidator(h -> rejectWith.get());
                    Sender.LineSenderBuilder b = Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .reconnectInitialBackoffMillis(10)
                            .reconnectMaxBackoffMillis(20)
                            .errorHandler(errors)
                            .httpTokenProvider(() -> "TOKEN");
                    if (armed) {
                        b.authFailureMaxDurationMillis(800);
                    }
                    try (Sender sender = b.build()) {
                        for (int outage = 0; outage < 2; outage++) {
                            rejectWith.set(401);
                            server.dropAllConnections();
                            Thread.sleep(500); // under the deadline
                            rejectWith.set(0); // a successful upgrade ends the outage
                            final long acked = handler.frames.get();
                            sender.table("t").longColumn("v", outage).atNow();
                            sender.flush();
                            await(() -> handler.frames.get() > acked, 10_000, "recovery");
                        }
                        Assert.assertEquals("two outages of 500 ms each, the clock reset in between [armed=" + armed + ']',
                                0, errors.terminalCount());
                        if (!armed) {
                            rejectWith.set(401);
                            server.dropAllConnections();
                            Thread.sleep(1_500);
                            Assert.assertEquals("no deadline: retried indefinitely", 0, errors.terminalCount());
                            Assert.assertTrue(errors.count("auth-rejected") > 0);
                            rejectWith.set(0);
                        }
                    }
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

    private static SenderError terminal(ErrorCollector errors) {
        for (SenderError e : errors.errors) {
            if (e.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                return e;
            }
        }
        throw new AssertionError("no terminal error");
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
}
