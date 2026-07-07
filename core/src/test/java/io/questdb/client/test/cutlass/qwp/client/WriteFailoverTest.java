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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end tests for write-side failover across multiple {@code addr=}
 * entries. Exercises the {@code X-QuestDB-Role} negotiation introduced in
 * questdb#7061 against {@code /write/v4} upgrades:
 * <ul>
 *   <li>{@code 101 + X-QuestDB-Role: PRIMARY|STANDALONE} — accept and stream</li>
 *   <li>{@code 421 Misdirected Request + X-QuestDB-Role: REPLICA|PRIMARY_CATCHUP}
 *       — close, rotate, retry the next configured address</li>
 * </ul>
 * The reconnect budget caps how long a sender will keep walking the list
 * before surfacing the failure; when every attempt within the budget hit
 * a non-writable role, the surfaced error is
 * {@link QwpRoleMismatchException}, distinguishing "no PRIMARY available"
 * from "all endpoints unreachable".
 */
public class WriteFailoverTest {

    @Test
    public void testAuthTimeoutBoundsHungUpgrade() throws Exception {
        // Per spec, auth_timeout_ms bounds each WebSocket upgrade. A
        // server that accepts the TCP connection but never sends a 101
        // response should NOT burn the entire reconnect budget on a
        // single host — the upgrade times out per-host, the sender
        // moves on. Use a no-op listener that just accepts and parks.
        int hangPort;
        int goodPort;
        try (java.net.ServerSocket hangListener = new java.net.ServerSocket(0)) {
            hangPort = hangListener.getLocalPort();
            // Park accepted sockets so the WebSocket upgrade never gets
            // a 101 response. Daemon thread so the JVM can exit.
            Thread acceptor = new Thread(() -> {
                try {
                    while (!hangListener.isClosed()) {
                        java.net.Socket s = hangListener.accept();
                        // Hold the socket open without writing anything.
                        // Test cleanup closes the listener to release.
                        s.setSoTimeout(60_000);
                    }
                } catch (IOException ignored) {
                    // listener closed
                }
            }, "hang-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();

            AckHandler ack = new AckHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(ack, false, "PRIMARY")) {
                goodPort = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                long t0 = System.nanoTime();
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + hangPort)
                        .address("localhost:" + goodPort)
                        .authTimeoutMillis(500)
                        .build()) {
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> ack.totalBinary.get() >= 1, 5_000);
                }
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                // hangPort exhausts auth_timeout_ms (500ms) then we move to
                // goodPort and connect. Should be well under the legacy
                // default-15s timeout; allow generous slack for CI.
                Assert.assertTrue(
                        "expected auth_timeout_ms to bound the hang (~500ms) but elapsed=" + elapsedMs + "ms",
                        elapsedMs < 5_000L);
            }
        } catch (IOException ignored) {
            // best-effort
        }
    }

    @Test
    public void testFailoverPastReplicaToPrimary() throws Exception {
        // Two servers: server1 always rejects with 421 + REPLICA, server2
        // accepts with 101 + PRIMARY. The sender's connect path must walk
        // past server1 and land on server2 within the reconnect budget.
        AckHandler ack = new AckHandler();
        TestWebSocketServer replica = new TestWebSocketServer(ack);
        int port1 = replica.getPort();
        replica.setRejectWithRole("REPLICA");
        TestWebSocketServer primary = new TestWebSocketServer(ack, false, "PRIMARY");
        int port2 = primary.getPort();
        try {
            replica.start();
            primary.start();
            Assert.assertTrue(replica.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(primary.awaitStart(5, TimeUnit.SECONDS));

            // Off-mode (default) walks every host once with no inter-host
            // backoff. Multi-host failover doesn't need SYNC/sf_dir.
            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .build()) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> ack.totalBinary.get() >= 1, 5_000);
            }
        } finally {
            replica.close();
            primary.close();
        }
    }

    @Test
    public void testFailoverPromotedReplicaJoinsRotation() throws Exception {
        // Failover during a real failover window: server1 starts as REPLICA
        // (rejects), server2 starts as PRIMARY (accepts). Mid-test, server1
        // is promoted (clear the reject) and we verify the sender stays on
        // server2 — currentAddressIndex stickiness means we don't rotate
        // off a healthy primary just because another node became writable.
        AckHandler ack = new AckHandler();
        TestWebSocketServer s1 = new TestWebSocketServer(ack);
        int port1 = s1.getPort();
        s1.setRejectWithRole("REPLICA");
        TestWebSocketServer s2 = new TestWebSocketServer(ack, false, "PRIMARY");
        int port2 = s2.getPort();
        try {
            s1.start();
            s2.start();
            Assert.assertTrue(s1.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(s2.awaitStart(5, TimeUnit.SECONDS));

            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .build()) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> ack.totalBinary.get() >= 1, 5_000);

                // Promote server1 (no longer rejects). Sender should stay
                // on server2 because the primary it landed on is still
                // healthy — rotation only happens on failure.
                s1.setRejectWithRole(null);
                s1.setAdvertisedRole("PRIMARY");

                long beforeBatch2 = ack.totalBinary.get();
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> ack.totalBinary.get() > beforeBatch2, 5_000);
            }
        } finally {
            s1.close();
            s2.close();
        }
    }

    @Test
    public void testOffModeSinglePassExhaustionThrowsRoleMismatch() throws Exception {
        // Off-mode walk that hits only replicas must surface
        // QwpRoleMismatchException — the walked address list resembles
        // a deployment mid-failover, distinguishable from "all hosts down".
        TestWebSocketServer r1 = new TestWebSocketServer(new AckHandler());
        int port1 = r1.getPort();
        r1.setRejectWithRole("REPLICA");
        TestWebSocketServer r2 = new TestWebSocketServer(new AckHandler());
        int port2 = r2.getPort();
        r2.setRejectWithRole("PRIMARY_CATCHUP");
        try {
            r1.start();
            r2.start();
            Assert.assertTrue(r1.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(r2.awaitStart(5, TimeUnit.SECONDS));

            try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .build()) {
                Assert.fail("expected QwpRoleMismatchException after off-mode walk");
            } catch (QwpRoleMismatchException e) {
                Assert.assertEquals("PRIMARY", e.getTargetRole());
                String msg = e.getMessage();
                Assert.assertTrue("should mention single-pass walk: " + msg,
                        msg.contains("walked"));
                Assert.assertTrue("should mention an unsuitable role: " + msg,
                        msg.contains("REPLICA") || msg.contains("PRIMARY_CATCHUP"));
            }
        } finally {
            r1.close();
            r2.close();
        }
    }

    @Test
    public void testOffModeSinglePassWalkFindsPrimary() throws Exception {
        // failover.md §1.2/§4.2: with initial_connect_retry=off (the
        // default), the engine still walks the full address list once
        // with NO inter-host backoff; only after every host has been
        // tried does it fail terminally. Java's prior off-mode tried
        // hosts[0] alone.
        AckHandler ack = new AckHandler();
        TestWebSocketServer replica = new TestWebSocketServer(ack);
        int port1 = replica.getPort();
        replica.setRejectWithRole("REPLICA");
        TestWebSocketServer primary = new TestWebSocketServer(ack, false, "PRIMARY");
        int port2 = primary.getPort();
        try {
            replica.start();
            primary.start();
            Assert.assertTrue(replica.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(primary.awaitStart(5, TimeUnit.SECONDS));

            // No initialConnectMode call — exercise the default (OFF).
            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .build()) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> ack.totalBinary.get() >= 1, 5_000);
            }
        } finally {
            replica.close();
            primary.close();
        }
    }

    @Test
    public void testRoleMismatchExceptionWhenAllReplicas() throws Exception {
        // Every configured address is a REPLICA. The sender must surface
        // QwpRoleMismatchException (not a generic LineSenderException) so
        // operators can distinguish "no primary elected yet" from
        // "everything is down".
        AckHandler ack = new AckHandler();
        TestWebSocketServer r1 = new TestWebSocketServer(ack);
        int port1 = r1.getPort();
        r1.setRejectWithRole("REPLICA");
        TestWebSocketServer r2 = new TestWebSocketServer(ack);
        int port2 = r2.getPort();
        r2.setRejectWithRole("PRIMARY_CATCHUP");
        try {
            r1.start();
            r2.start();
            Assert.assertTrue(r1.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(r2.awaitStart(5, TimeUnit.SECONDS));

            QwpRoleMismatchException observed = null;
            // Off-mode walk surfaces QwpRoleMismatchException on round
            // exhaustion when every host responded with a non-writable role.
            try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .build()) {
                Assert.fail("expected QwpRoleMismatchException, sender connected");
            } catch (QwpRoleMismatchException e) {
                observed = e;
            } catch (LineSenderException e) {
                // Surface the wrong type as a clear test failure
                throw new AssertionError(
                        "expected QwpRoleMismatchException but got LineSenderException: "
                                + e.getMessage(), e);
            }
            Assert.assertNotNull("expected a role-mismatch surface", observed);
            Assert.assertEquals("PRIMARY", observed.getTargetRole());
            String msg = observed.getMessage();
            Assert.assertNotNull(msg);
            Assert.assertTrue("error must mention the unsuitable role: " + msg,
                    msg.contains("REPLICA") || msg.contains("PRIMARY_CATCHUP"));
        } finally {
            r1.close();
            r2.close();
        }
    }

    @Test
    public void testMixedSweepRoleRejectOutranksLatchedTerminalUpgradeError() throws Exception {
        // Mixed connect sweep: [replica(421+role), replica(421+role),
        // node(503)]. The co-occurring 421 role rejects are positive
        // evidence of a transient failover/promotion window -- a replica
        // can be promoted and a primary will reappear -- so the round
        // epilogue must surface the retriable QwpRoleMismatchException,
        // NOT the latched non-421 terminal upgrade error. Preferring the
        // 503 turns a transient window into a dead sender (or a drainer
        // slot quarantine on the background path).
        AckHandler ack = new AckHandler();
        TestWebSocketServer r1 = new TestWebSocketServer(ack);
        int port1 = r1.getPort();
        r1.setRejectWithRole("REPLICA");
        TestWebSocketServer r2 = new TestWebSocketServer(ack);
        int port2 = r2.getPort();
        r2.setRejectWithRole("REPLICA");
        TestWebSocketServer sick = new TestWebSocketServer(ack);
        int port3 = sick.getPort();
        sick.setRejectWithStatus(503, "Service Unavailable");
        try {
            r1.start();
            r2.start();
            sick.start();
            Assert.assertTrue(r1.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(r2.awaitStart(5, TimeUnit.SECONDS));
            Assert.assertTrue(sick.awaitStart(5, TimeUnit.SECONDS));

            QwpRoleMismatchException observed = null;
            // Off-mode walk sweeps every endpoint once and classifies the
            // exhausted round at build(). Endpoint pick order does not
            // matter: 421 role rejects and the 503 both `continue` the walk,
            // so all three endpoints are always visited before the epilogue.
            try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port1)
                    .address("localhost:" + port2)
                    .address("localhost:" + port3)
                    .build()) {
                Assert.fail("expected the mixed-sweep connect to fail");
            } catch (QwpRoleMismatchException e) {
                observed = e;
            } catch (WebSocketUpgradeException e) {
                throw new AssertionError(
                        "mixed sweep misclassified as terminal: latched non-421 upgrade error "
                                + "(status=" + e.getStatusCode()
                                + ") outranked role-reject evidence: " + e.getMessage(), e);
            } catch (LineSenderException e) {
                throw new AssertionError(
                        "expected QwpRoleMismatchException but got LineSenderException: "
                                + e.getMessage(), e);
            }
            Assert.assertNotNull("expected a role-mismatch surface", observed);
            Assert.assertEquals("PRIMARY", observed.getTargetRole());
            String msg = observed.getMessage();
            Assert.assertNotNull(msg);
            Assert.assertTrue("error must mention the observed replica role: " + msg,
                    msg.contains("REPLICA"));
            // The demoted non-421 upgrade error must stay observable for
            // diagnostics: it rides along as a suppressed exception on the
            // surfaced role-mismatch classification.
            WebSocketUpgradeException demoted = null;
            for (Throwable s : observed.getSuppressed()) {
                if (s instanceof WebSocketUpgradeException) {
                    demoted = (WebSocketUpgradeException) s;
                    break;
                }
            }
            Assert.assertNotNull(
                    "expected the demoted 503 upgrade error as a suppressed diagnostic",
                    demoted);
            Assert.assertEquals(503, demoted.getStatusCode());
        } finally {
            r1.close();
            r2.close();
            sick.close();
        }
    }

    @Test
    public void testStandaloneIsTreatedAsWritable() throws Exception {
        // OSS / single-node deployments advertise STANDALONE. The client
        // must accept that handshake without rotation since standalone
        // nodes are writable.
        AckHandler ack = new AckHandler();
        try (TestWebSocketServer standalone = new TestWebSocketServer(ack, false, "STANDALONE")) {
            int port = standalone.getPort();
            standalone.start();
            Assert.assertTrue(standalone.awaitStart(5, TimeUnit.SECONDS));

            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port)
                    .build()) {
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();
                waitFor(() -> ack.totalBinary.get() >= 1, 5_000);
            }
        }
    }

    @Test
    public void testUpgradeException421CarriesRoleHeader() throws Exception {
        // A 421 response with X-QuestDB-Role: REPLICA must surface a typed
        // WebSocketUpgradeException somewhere on the thrown exception's
        // cause chain, with the parsed status code and role exposed for
        // diagnostics. With a single-replica config the off-mode walk
        // wraps it in QwpRoleMismatchException; the WebSocketUpgradeException
        // sits on the cause chain via initCause().
        TestWebSocketServer replica = new TestWebSocketServer(new AckHandler());
        int port = replica.getPort();
        try (TestWebSocketServer replicaResource = replica) {
            replica.setRejectWithRole("REPLICA");
            replica.start();
            Assert.assertTrue(replica.awaitStart(5, TimeUnit.SECONDS));

            Throwable thrown = null;
            try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + port)
                    .build()) {
                Assert.fail("expected the 421 reject to abort connect");
            } catch (Throwable t) {
                thrown = t;
            }
            Assert.assertNotNull(thrown);
            // Single-pass walk of one replica → terminal QwpRoleMismatchException.
            Assert.assertTrue("expected QwpRoleMismatchException, got " + thrown.getClass(),
                    thrown instanceof QwpRoleMismatchException);
            WebSocketUpgradeException upgrade = findUpgradeOnChain(thrown);
            Assert.assertNotNull(
                    "expected WebSocketUpgradeException on cause chain: " + thrown,
                    upgrade);
            Assert.assertEquals(421, upgrade.getStatusCode());
            Assert.assertEquals("REPLICA", upgrade.getServerRole());
            Assert.assertTrue("isRoleMismatch() must be true for 421", upgrade.isRoleMismatch());
        }
    }

    private static WebSocketUpgradeException findUpgradeOnChain(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof WebSocketUpgradeException) {
                return (WebSocketUpgradeException) cur;
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return null;
    }

    private static void waitFor(BooleanProbe probe, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (probe.test()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.fail("condition not met within " + timeoutMs + "ms");
    }

    private interface BooleanProbe {
        boolean test();
    }

    /**
     * Minimal ACK handler shared across tests; keeps senders unblocked.
     */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinary = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinary.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // STATUS_OK | sequence | table_count(0) — see WebSocketResponse.
        private static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
