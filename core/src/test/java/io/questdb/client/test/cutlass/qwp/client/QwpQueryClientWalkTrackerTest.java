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
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Integration coverage for the WalkTracker helper inside
 * {@link QwpQueryClient}. The helper is private; this test exercises it
 * indirectly through {@link QwpQueryClient#connect()}, which is the
 * spec-mandated public surface of the WalkTracker (failover.md §4.4).
 * <p>
 * The asserted behaviours mirror the spec table:
 * <ul>
 *   <li>Walk picks the first reachable host that satisfies the {@code
 *       target=} filter; classifies skipped hosts so subsequent walks see
 *       them at lower priority.</li>
 *   <li>421 + {@code X-QuestDB-Role: REPLICA} → {@code TopologyReject},
 *       walk continues; 421 + {@code PRIMARY_CATCHUP} → {@code
 *       TransientReject}, walk continues.</li>
 *   <li>HTTP 401 / 403 are terminal AT THE FIRST HOST -- the walk does NOT
 *       continue (failover.md §6 AuthError).</li>
 *   <li>Pure transport failures (refused TCP, etc.) drive {@code
 *       TransportError} and the walk continues to the next host.</li>
 *   <li>When no endpoint matches, the surfaced exception type
 *       distinguishes "no role match" ({@link QwpRoleMismatchException})
 *       from "all unreachable" ({@link HttpClientException}).</li>
 * </ul>
 * <p>
 * SERVER_INFO-driven role checks (target=primary against a v2 server
 * advertising REPLICA via the SERVER_INFO frame) belong to the parent
 * QuestDB egress integration suite -- TestWebSocketServer here only
 * covers the upgrade-time {@code X-QuestDB-Role} header path which is
 * sufficient for WalkTracker's classification logic.
 */
public class QwpQueryClientWalkTrackerTest {

    // Disjoint port windows from existing failover tests so concurrent
    // runs don't collide on bind addresses.
    private static final int BASE_PORT = 22_300 + (int) (System.nanoTime() % 100);
    private static final TestWebSocketServer.WebSocketServerHandler NOOP_HANDLER =
            new TestWebSocketServer.WebSocketServerHandler() {
                // default onBinaryMessage is fine
            };

    @Test
    public void testWalk_404NotFoundIsTransportNotTerminal() throws Exception {
        // 404 (per failover.md §4.1: a single mid-deploy node serving the
        // wrong path while peers are healthy is a routing glitch, not an
        // auth failure). Walk must continue.
        int port404 = BASE_PORT + 360;
        int portOk = BASE_PORT + 361;
        TestWebSocketServer notFound = new TestWebSocketServer(port404, NOOP_HANDLER);
        notFound.setRejectWithStatus(404, "Not Found");
        TestWebSocketServer ok = new TestWebSocketServer(portOk, NOOP_HANDLER);
        try {
            notFound.start();
            ok.start();
            Assert.assertTrue(notFound.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port404 + ",localhost:" + portOk + ";auth_timeout_ms=2000;")) {
                client.connect();
                Assert.assertTrue("client must walk past 404", client.isConnected());
            }
        } finally {
            notFound.close();
            ok.close();
        }
    }

    @Test
    public void testWalk_426UpgradeRequiredIsTransportNotTerminal() throws Exception {
        // 426 Upgrade Required is per failover.md §6 a transient/transport
        // failure (NOT terminal). The walk must continue to the next host.
        int port426 = BASE_PORT + 340;
        int portOk = BASE_PORT + 341;
        TestWebSocketServer rejecting = new TestWebSocketServer(port426, NOOP_HANDLER);
        rejecting.setRejectWithStatus(426, "Upgrade Required");
        TestWebSocketServer ok = new TestWebSocketServer(portOk, NOOP_HANDLER);
        try {
            rejecting.start();
            ok.start();
            Assert.assertTrue(rejecting.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port426 + ",localhost:" + portOk + ";auth_timeout_ms=2000;")) {
                client.connect();
                Assert.assertTrue("client must walk past 426 to the second host", client.isConnected());
            }
        } finally {
            rejecting.close();
            ok.close();
        }
    }

    @Test
    public void testWalk_AllReplicasThrowsRoleMismatch() throws Exception {
        // Two REPLICA-rejecting endpoints with target=primary: the walk
        // exhausts, fall-through reset re-walks (rehabilitating stale
        // TopologyRejects from prior outages -- here there are none),
        // exhausts again, and surfaces QwpRoleMismatchException with the
        // last observed role attached.
        int port1 = BASE_PORT + 260;
        int port2 = BASE_PORT + 261;
        TestWebSocketServer rep1 = new TestWebSocketServer(port1, NOOP_HANDLER);
        rep1.setRejectWithRole("REPLICA");
        TestWebSocketServer rep2 = new TestWebSocketServer(port2, NOOP_HANDLER);
        rep2.setRejectWithRole("REPLICA");
        try {
            rep1.start();
            rep2.start();
            Assert.assertTrue(rep1.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(rep2.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port1 + ",localhost:" + port2 + ";target=primary;auth_timeout_ms=2000;")) {
                try {
                    client.connect();
                    Assert.fail("expected QwpRoleMismatchException");
                } catch (QwpRoleMismatchException expected) {
                    // Pinned in the message: spec-mandated wording so
                    // downstream tooling can disambiguate "no primary" from
                    // "all unreachable".
                    Assert.assertTrue("message must mention target=primary: " + expected.getMessage(),
                            expected.getMessage().contains("target=primary"));
                }
            }
        } finally {
            rep1.close();
            rep2.close();
        }
    }

    @Test
    public void testWalk_AllUnreachableThrowsHttpClientException() {
        // No server bound on either port. Both attempts return TCP refused.
        // The exception type is HttpClientException (transport-only
        // failure mode) -- distinct from QwpRoleMismatchException which
        // would falsely suggest a topology issue.
        int port1 = BASE_PORT + 200;
        int port2 = BASE_PORT + 201;
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=localhost:" + port1 + ",localhost:" + port2 + ";auth_timeout_ms=300;")) {
            try {
                client.connect();
                Assert.fail("expected HttpClientException on unreachable hosts");
            } catch (HttpClientException expected) {
                Assert.assertTrue("message must call out endpoint count: " + expected.getMessage(),
                        expected.getMessage().contains("[count=2"));
            }
        }
    }

    @Test
    public void testWalk_AuthFailure403IsTerminal() throws Exception {
        // 403 is symmetric to 401: same terminal classification.
        int port403 = BASE_PORT + 240;
        int portOk = BASE_PORT + 241;
        TestWebSocketServer forbidden = new TestWebSocketServer(port403, NOOP_HANDLER);
        forbidden.setRejectWithStatus(403, "Forbidden");
        TestWebSocketServer ok = new TestWebSocketServer(portOk, NOOP_HANDLER);
        try {
            forbidden.start();
            ok.start();
            Assert.assertTrue(forbidden.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port403 + ",localhost:" + portOk + ";auth_timeout_ms=2000;")) {
                try {
                    client.connect();
                    Assert.fail("expected QwpAuthFailedException on 403");
                } catch (QwpAuthFailedException expected) {
                    Assert.assertEquals(403, expected.getStatusCode());
                }
            }
        } finally {
            forbidden.close();
            ok.close();
        }
    }

    @Test
    public void testWalk_AuthFailureFirstHostIsTerminal() throws Exception {
        // A 401 at the FIRST reachable host MUST surface immediately.
        // Without the WalkTracker's auth-terminal classification the
        // loop would continue to the second host, producing a
        // QwpRoleMismatchException or accepting the second host -- both
        // mask the credential failure (failover.md §6 AuthError).
        int port401 = BASE_PORT + 220;
        int portOk = BASE_PORT + 221;
        TestWebSocketServer auth = new TestWebSocketServer(port401, NOOP_HANDLER);
        auth.setRejectWithStatus(401, "Unauthorized");
        TestWebSocketServer ok = new TestWebSocketServer(portOk, NOOP_HANDLER);
        try {
            auth.start();
            ok.start();
            Assert.assertTrue(auth.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port401 + ",localhost:" + portOk + ";auth_timeout_ms=2000;")) {
                try {
                    client.connect();
                    Assert.fail("expected QwpAuthFailedException on 401");
                } catch (QwpAuthFailedException expected) {
                    Assert.assertEquals(401, expected.getStatusCode());
                }
                Assert.assertFalse("client must NOT be bound after terminal auth failure",
                        client.isConnected());
            }
        } finally {
            auth.close();
            ok.close();
        }
    }

    @Test
    public void testWalk_FallThroughResetRehabilitatesPriorTopologyRejects() throws Exception {
        // Cross-connect scenario: a prior connect classified both hosts
        // as TopologyReject (REPLICA). Then host A's rejection clears
        // (a real failover) and a new connect is attempted on the same
        // client. The WalkTracker fall-through reset MUST rehabilitate
        // the prior classifications so A can be reconsidered.
        //
        // Without the fall-through reset, the second connect would see
        // every host attempted=true (carried over) and short-circuit; or
        // see every host TopologyReject (priority 5) and walk past the
        // now-healthy A only to fail.
        //
        // target=any keeps this v1-friendly: the spec defines
        // target=primary as requiring v2 SERVER_INFO, which the test
        // server doesn't emit. A separate integration test in the
        // parent QuestDB repo covers the SERVER_INFO path.
        int portA = BASE_PORT + 320;
        int portB = BASE_PORT + 321;
        TestWebSocketServer a = new TestWebSocketServer(portA, NOOP_HANDLER);
        a.setRejectWithRole("REPLICA");
        TestWebSocketServer b = new TestWebSocketServer(portB, NOOP_HANDLER);
        b.setRejectWithRole("REPLICA");
        try {
            a.start();
            b.start();
            Assert.assertTrue(a.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(b.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + portA + ",localhost:" + portB + ";auth_timeout_ms=2000;")) {
                // First connect: both REPLICA → role mismatch.
                try {
                    client.connect();
                    Assert.fail("expected QwpRoleMismatchException on first connect");
                } catch (QwpRoleMismatchException ignored) {
                    // expected
                }
                // Clear A's rejection: it now responds with a clean 101
                // upgrade. The fall-through reset on the next walk
                // rehabilitates the prior classification.
                a.setRejectWithRole(null);
                client.connect();
                Assert.assertTrue("client must bind A after rejection cleared", client.isConnected());
            }
        } finally {
            a.close();
            b.close();
        }
    }

    @Test
    public void testWalk_FirstReachablePrimaryWins() throws Exception {
        // First host is REPLICA-rejecting; second is a PRIMARY-advertising
        // server. WalkTracker must skip the first and bind to the second.
        int portReplica = BASE_PORT + 280;
        int portPrimary = BASE_PORT + 281;
        TestWebSocketServer rep = new TestWebSocketServer(portReplica, NOOP_HANDLER);
        rep.setRejectWithRole("REPLICA");
        TestWebSocketServer prim = new TestWebSocketServer(portPrimary, NOOP_HANDLER, false, "PRIMARY");
        try {
            rep.start();
            prim.start();
            Assert.assertTrue(rep.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(prim.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + portReplica + ",localhost:" + portPrimary + ";auth_timeout_ms=2000;")) {
                client.connect();
                Assert.assertTrue("client must be connected after walk", client.isConnected());
            }
        } finally {
            rep.close();
            prim.close();
        }
    }

    @Test
    public void testWalk_TransportFailureContinuesWalk() throws Exception {
        // First port has no server (TCP refused); second is reachable.
        // WalkTracker must classify the first as TransportError and bind
        // the second on the same walk (no fall-through reset needed yet).
        int portDead = BASE_PORT + 300;
        int portOk = BASE_PORT + 301;
        try (TestWebSocketServer ok = new TestWebSocketServer(portOk, NOOP_HANDLER)) {
            ok.start();
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + portDead + ",localhost:" + portOk + ";auth_timeout_ms=500;")) {
                client.connect();
                Assert.assertTrue("client must bind to second host", client.isConnected());
            }
        }
    }
}
