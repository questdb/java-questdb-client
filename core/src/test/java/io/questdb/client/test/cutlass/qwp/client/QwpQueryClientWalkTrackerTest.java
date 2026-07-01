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
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
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
 * SERVER_INFO-driven role checks (target=primary against a server
 * advertising REPLICA via the SERVER_INFO frame) belong to the parent
 * QuestDB egress integration suite -- TestWebSocketServer here only
 * covers the upgrade-time {@code X-QuestDB-Role} header path which is
 * sufficient for WalkTracker's classification logic.
 */
public class QwpQueryClientWalkTrackerTest {

    private static final TestWebSocketServer.WebSocketServerHandler NOOP_HANDLER =
            new TestWebSocketServer.WebSocketServerHandler() {
                // default onBinaryMessage is fine
            };

    @Test
    public void testWalk_404NotFoundIsTransportNotTerminal() throws Exception {
        // 404 (per failover.md §4.1: a single mid-deploy node serving the
        // wrong path while peers are healthy is a routing glitch, not an
        // auth failure). Walk must continue.
        TestWebSocketServer notFound = new TestWebSocketServer(NOOP_HANDLER);
        notFound.setRejectWithStatus(404, "Not Found");
        int port404 = notFound.getPort();
        TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER);
        ok.setSendServerInfo(true);
        int portOk = ok.getPort();
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
        TestWebSocketServer rejecting = new TestWebSocketServer(NOOP_HANDLER);
        rejecting.setRejectWithStatus(426, "Upgrade Required");
        int port426 = rejecting.getPort();
        TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER);
        ok.setSendServerInfo(true);
        int portOk = ok.getPort();
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
        TestWebSocketServer rep1 = new TestWebSocketServer(NOOP_HANDLER);
        rep1.setRejectWithRole("REPLICA");
        int port1 = rep1.getPort();
        TestWebSocketServer rep2 = new TestWebSocketServer(NOOP_HANDLER);
        rep2.setRejectWithRole("REPLICA");
        int port2 = rep2.getPort();
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
        // findUnusedPorts (plural) holds both probe sockets open at once so
        // the two ports are guaranteed distinct — two separate
        // findUnusedPort() calls can return the SAME port (bind-close-return
        // lets the kernel recycle it immediately), which fails the config's
        // duplicate-addr validation before the walk under test even runs.
        int[] ports = TestPorts.findUnusedPorts(2);
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=localhost:" + ports[0] + ",localhost:" + ports[1] + ";auth_timeout_ms=300;")) {
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
        TestWebSocketServer forbidden = new TestWebSocketServer(NOOP_HANDLER);
        forbidden.setRejectWithStatus(403, "Forbidden");
        int port403 = forbidden.getPort();
        TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER);
        int portOk = ok.getPort();
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
        TestWebSocketServer auth = new TestWebSocketServer(NOOP_HANDLER);
        auth.setRejectWithStatus(401, "Unauthorized");
        int port401 = auth.getPort();
        TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER);
        int portOk = ok.getPort();
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
        // target=any so the rehabilitated host A binds regardless of the
        // role it advertises in its SERVER_INFO frame -- this test
        // exercises the fall-through reset, not the role filter. The
        // SERVER_INFO-driven role filter (target=primary/replica) is
        // covered by a separate integration test in the parent QuestDB repo.
        TestWebSocketServer a = new TestWebSocketServer(NOOP_HANDLER);
        a.setRejectWithRole("REPLICA");
        a.setSendServerInfo(true);
        int portA = a.getPort();
        TestWebSocketServer b = new TestWebSocketServer(NOOP_HANDLER);
        b.setRejectWithRole("REPLICA");
        int portB = b.getPort();
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
        TestWebSocketServer rep = new TestWebSocketServer(NOOP_HANDLER);
        rep.setRejectWithRole("REPLICA");
        int portReplica = rep.getPort();
        TestWebSocketServer prim = new TestWebSocketServer(NOOP_HANDLER, false, "PRIMARY");
        prim.setSendServerInfo(true);
        int portPrimary = prim.getPort();
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
    public void testWalk_ServerInfoReplicaRejectedForTargetPrimary() throws Exception {
        // A node that completes a clean 101 upgrade and then advertises REPLICA
        // only in its SERVER_INFO frame (not via the 421 X-QuestDB-Role header
        // path the other role tests use) must be rejected by the role filter
        // when target=primary. Pins matchesTarget(info.getRole(), target) where
        // info is the decoded SERVER_INFO -- the branch that outlived the
        // v1-mismatch removal. A clean 101 ignores the upgrade-time role header,
        // so the rejection here is driven purely by the SERVER_INFO role.
        TestWebSocketServer replica = new TestWebSocketServer(NOOP_HANDLER);
        replica.setAdvertisedRole("REPLICA");
        replica.setSendServerInfo(true);
        int port = replica.getPort();
        try {
            replica.start();
            Assert.assertTrue(replica.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port + ";target=primary;auth_timeout_ms=2000;")) {
                try {
                    client.connect();
                    Assert.fail("expected QwpRoleMismatchException for a REPLICA SERVER_INFO under target=primary");
                } catch (QwpRoleMismatchException expected) {
                    Assert.assertFalse("a role mismatch must not leave the client connected", client.isConnected());
                    Assert.assertEquals("primary", expected.getTargetRole());
                    // The rejected role is taken from the decoded SERVER_INFO frame.
                    Assert.assertNotNull("observed SERVER_INFO must be attached", expected.getLastObserved());
                    Assert.assertEquals(QwpEgressMsgKind.ROLE_REPLICA, expected.getLastObserved().getRole());
                    Assert.assertTrue("message must mention target=primary: " + expected.getMessage(),
                            expected.getMessage().contains("target=primary"));
                }
            }
        } finally {
            replica.close();
        }
    }

    @Test
    public void testWalk_ServerInfoRoleFilterSkipsReplicaBindsPrimary() throws Exception {
        // Both endpoints complete a clean 101 and advertise their role only via
        // the SERVER_INFO frame. With target=primary the walk must skip the
        // REPLICA endpoint -- a SERVER_INFO role mismatch is a skip, not a
        // terminal failure -- and bind the PRIMARY one. Exercises the
        // walk-continues side of matchesTarget(info.getRole(), target).
        TestWebSocketServer replica = new TestWebSocketServer(NOOP_HANDLER);
        replica.setAdvertisedRole("REPLICA");
        replica.setSendServerInfo(true);
        int portReplica = replica.getPort();
        TestWebSocketServer primary = new TestWebSocketServer(NOOP_HANDLER);
        primary.setAdvertisedRole("PRIMARY");
        primary.setSendServerInfo(true);
        int portPrimary = primary.getPort();
        try {
            replica.start();
            primary.start();
            Assert.assertTrue(replica.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(primary.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + portReplica + ",localhost:" + portPrimary
                            + ";target=primary;auth_timeout_ms=2000;")) {
                client.connect();
                Assert.assertTrue("client must skip the REPLICA and bind the PRIMARY", client.isConnected());
                Assert.assertNotNull("bound connection must carry SERVER_INFO", client.getServerInfo());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY, client.getServerInfo().getRole());
            }
        } finally {
            replica.close();
            primary.close();
        }
    }

    @Test(timeout = 15_000)
    public void testWalk_ServerInfoTimeoutIsTransportNotTerminal() throws Exception {
        // A node that completes the 101 upgrade but never sends the mandatory
        // SERVER_INFO frame must be treated as a transport error so the walk
        // continues, not a terminal failure. receiveServerInfoSync() now runs on
        // every connect, so a silent post-upgrade peer would otherwise stall the
        // client until the server-info timeout; bound it short here and verify
        // the walk falls through to a healthy node.
        TestWebSocketServer silent = new TestWebSocketServer(NOOP_HANDLER);
        int portSilent = silent.getPort();
        // sendServerInfo left off: the 101 upgrade succeeds, then the node stays silent.
        TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER);
        ok.setSendServerInfo(true);
        int portOk = ok.getPort();
        try {
            silent.start();
            ok.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(ok.awaitStart(5, TimeUnit.SECONDS));

            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + portSilent + ",localhost:" + portOk + ";auth_timeout_ms=2000;")) {
                client.withServerInfoTimeout(300);
                client.connect();
                Assert.assertTrue("client must walk past the SERVER_INFO-silent node", client.isConnected());
            }
        } finally {
            silent.close();
            ok.close();
        }
    }

    @Test
    public void testWalk_TransportFailureContinuesWalk() throws Exception {
        // First port has no server (TCP refused); second is reachable.
        // WalkTracker must classify the first as TransportError and bind
        // the second on the same walk (no fall-through reset needed yet).
        int portDead = TestPorts.findUnusedPort();
        try (TestWebSocketServer ok = new TestWebSocketServer(NOOP_HANDLER)) {
            ok.setSendServerInfo(true);
            int portOk = ok.getPort();
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
