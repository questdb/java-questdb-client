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

import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The failures {@link QwpQueryClient#execute} detects itself -- a failover
 * reconnect that fails or meets a rejected or unavailable credential, a
 * failover that runs out of attempts, a stop request or a {@code close()}
 * between attempts -- reach the handler through the position-aware
 * {@code onError(requestId, status, message, position)} with
 * {@link QwpEgressMsgKind#NO_ERROR_POSITION}, like the server's errors, so a
 * handler that overrides only that overload sees them all. Each query asks for
 * the details; the first one dies with its connection, which sends
 * {@code execute()} down its failover path.
 */
public class QwpQueryClientFailurePathsTest {
    private static final int ALL_CAPABILITIES = QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_DETAILS;
    private static final QwpQueryOptions OPTIONS = new QwpQueryOptions().countRows(true).stats(true).errorPosition(true);
    private static final String SQL = "SELECT x FROM t";

    @Test(timeout = 30_000)
    public void testAuthRejectedFailoverReportsNoPosition() throws Exception {
        // the server dies with the first query and rejects the credential of
        // the failover reconnect
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<QwpEgressMockServer> serverRef = new AtomicReference<>();
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, r -> {
                serverRef.get().setAuthorizationValidator(authorization -> 401);
                return null;
            });
                 QwpQueryClient client = newClient(server)) {
                serverRef.set(server);
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, OPTIONS);
                handler.assertOnlyError();
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertTrue(handler.errorMessage,
                        handler.errorMessage.startsWith("auth-rejected during failover reconnect [host=localhost:"));
                TestUtils.assertContains(handler.errorMessage, ", status=401, last error: ");
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertEquals("the failover reconnect must not complete", 1, server.handshakeCount());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseBetweenAttemptsReportsClosed() throws Exception {
        // close() from the failover callback: the next attempt finds no I/O
        // thread and ends the query instead of touching the freed client
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, dropFirstQuery());
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler() {
                    @Override
                    public void onFailoverReset(long requestId, QwpServerInfo newNode) {
                        super.onFailoverReset(requestId, newNode);
                        client.close();
                    }
                };
                client.execute(SQL, handler, OPTIONS);
                Assert.assertEquals(1, handler.failoverResetCount);
                handler.assertOnlyError();
                Assert.assertEquals(-1, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertEquals("QwpQueryClient is closed", handler.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertFalse(client.wasLastCloseTimedOut());
                Assert.assertEquals("the closed client must not replay the query", 1, server.getRequests().size());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCredentialUnavailableFailoverReportsNoPosition() throws Exception {
        // the token provider fails when the failover reconnect asks it for a
        // credential, so the reconnect contacts no endpoint
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger tokenPulls = new AtomicInteger();
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, dropFirstQuery());
                 QwpQueryClient client = newClient(server).withBearerTokenProvider(() -> {
                     if (tokenPulls.incrementAndGet() > 1) {
                         throw TokenUnavailableException.retryable("IdP unreachable");
                     }
                     return "TOKEN";
                 })) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, OPTIONS);
                handler.assertOnlyError();
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertTrue(handler.errorMessage, handler.errorMessage.startsWith("failover reconnect failed: "));
                TestUtils.assertContains(handler.errorMessage, "IdP unreachable");
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertEquals("the failover reconnect must contact no endpoint", 1, server.handshakeCount());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testExhaustedFailoverReportsNoPosition() throws Exception {
        // failover_max_attempts=1 leaves no attempt for a failover: the query
        // dies with its connection, and the next query hits the latched failure
        TestUtils.assertMemoryLeak(() -> {
            final String failure = "transport failure after 1 execute attempt (0 failover reconnects); last error: ";
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, r -> null);
                 QwpQueryClient client = newClient(server, "failover_max_attempts=1;")) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, OPTIONS);
                handler.assertOnlyError();
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertTrue(handler.errorMessage, handler.errorMessage.startsWith(failure));
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);

                RecordingQueryHandler latched = new RecordingQueryHandler();
                client.execute(SQL, latched, OPTIONS);
                latched.assertOnlyError();
                Assert.assertEquals(-1, latched.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, latched.errorStatus);
                Assert.assertTrue(latched.errorMessage, latched.errorMessage.startsWith(failure));
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, latched.errorPosition);
                Assert.assertEquals("no failover reconnect", 1, server.handshakeCount());
                Assert.assertEquals("the latched failure sends nothing", 1, server.getRequests().size());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFailedReconnectsReportNoPosition() throws Exception {
        // The server dies with the first query and then refuses connections: the
        // failover reconnect fails, and so does the reconnect of the next query.
        // Once it accepts connections again, the client recovers.
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<QwpEgressMockServer> serverRef = new AtomicReference<>();
            AtomicInteger queries = new AtomicInteger();
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, r -> {
                if (queries.getAndIncrement() == 0) {
                    serverRef.get().setRejectUpgrades(true);
                    return null;
                }
                return answer(r);
            });
                 QwpQueryClient client = newClient(server)) {
                serverRef.set(server);
                client.connect();

                RecordingQueryHandler failover = new RecordingQueryHandler();
                client.execute(SQL, failover, OPTIONS);
                failover.assertOnlyError();
                Assert.assertEquals(server.getRequests().get(0).requestId, failover.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, failover.errorStatus);
                TestUtils.assertContains(failover.errorMessage, "failover reconnect failed after 1 attempt [last error: ");
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, failover.errorPosition);
                Assert.assertFalse(client.isConnected());

                RecordingQueryHandler reconnect = new RecordingQueryHandler();
                client.execute(SQL, reconnect, OPTIONS);
                reconnect.assertOnlyError();
                Assert.assertEquals(-1, reconnect.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, reconnect.errorStatus);
                Assert.assertTrue(reconnect.errorMessage, reconnect.errorMessage.startsWith("reconnect failed: "));
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, reconnect.errorPosition);

                server.setRejectUpgrades(false);
                RecordingQueryHandler recovered = new RecordingQueryHandler();
                client.execute(SQL, recovered, OPTIONS);
                Assert.assertEquals(1, recovered.endCount);
                Assert.assertEquals(Collections.singletonList(42L), recovered.values);
                Assert.assertEquals(1, recovered.rowCount);
                Assert.assertTrue(recovered.hasStats);
                Assert.assertEquals(2, server.getRequests().size());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testShutdownAfterTransportFailureReportsNoPosition() throws Exception {
        // a stop request that lands right after the connection dies ends the
        // query instead of failing over
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, r -> null);
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                client.setAfterTransportFailureHookForTest(client::requestShutdown);
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, OPTIONS);
                handler.assertOnlyError();
                Assert.assertEquals(0, handler.failoverResetCount);
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertEquals("query client shut down", handler.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertEquals("no failover reconnect after the stop request", 1, server.handshakeCount());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testShutdownBetweenAttemptsReportsNoPosition() throws Exception {
        // a stop request from the failover callback ends the query before the
        // replay
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, dropFirstQuery());
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler() {
                    @Override
                    public void onFailoverReset(long requestId, QwpServerInfo newNode) {
                        super.onFailoverReset(requestId, newNode);
                        client.requestShutdown();
                    }
                };
                client.execute(SQL, handler, OPTIONS);
                Assert.assertEquals(1, handler.failoverResetCount);
                handler.assertOnlyError();
                Assert.assertEquals(-1, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertEquals("query client shut down", handler.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertEquals("the query must not be replayed", 1, server.getRequests().size());
                server.assertNoFailure();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testShutdownDuringReconnectReportsNoPosition() throws Exception {
        // the stop request arrives while the failover reconnect waits for
        // SERVER_INFO: the reconnect completes, then the query ends
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<QwpQueryClient> clientRef = new AtomicReference<>();
            AtomicInteger serverInfos = new AtomicInteger();
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, dropFirstQuery());
                 QwpQueryClient client = newClient(server)) {
                clientRef.set(client);
                server.setBeforeServerInfoHook(() -> {
                    if (serverInfos.incrementAndGet() == 2) {
                        clientRef.get().requestShutdown();
                    }
                });
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, OPTIONS);
                handler.assertOnlyError();
                Assert.assertEquals(0, handler.failoverResetCount);
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                Assert.assertEquals("query client shut down", handler.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertEquals(2, server.handshakeCount());
                Assert.assertEquals("the query must not be replayed", 1, server.getRequests().size());
                server.assertNoFailure();
            }
        });
    }

    /**
     * One LONG row, x = 42, and the trailers the request asks for.
     */
    private static byte[][] answer(QwpEgressFrames.QueryRequest r) {
        QwpEgressFrames.Body end = QwpEgressFrames.resultEnd(r.requestId, 0, 1);
        if (r.has(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS)) {
            end.varint(1);
        }
        if (r.has(QwpEgressMsgKind.QUERY_FLAG_STATS)) {
            end.stats(1, 2, 3, 4, 0);
        }
        return new byte[][]{QwpEgressFrames.resultBatchOfLongs(r.requestId, 0, 42), end.toFrame()};
    }

    /**
     * Drops the connection on the first query, then answers like
     * {@link #answer}.
     */
    private static QwpEgressMockServer.Responder dropFirstQuery() {
        AtomicInteger queries = new AtomicInteger();
        return r -> queries.getAndIncrement() == 0 ? null : answer(r);
    }

    private static QwpQueryClient newClient(QwpEgressMockServer server) {
        return newClient(server, "");
    }

    private static QwpQueryClient newClient(QwpEgressMockServer server, String extraConfig) {
        return QwpQueryClient.fromConfig("ws::addr=" + server.getAddress()
                + ";auth_timeout_ms=2000;failover_backoff_initial_ms=0;" + extraConfig);
    }
}
