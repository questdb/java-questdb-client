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
import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies that the WebSocket (QWP) transport accepts an
 * {@link Sender.LineSenderBuilder#httpTokenProvider} and presents the provider's current token as the
 * {@code Authorization: Bearer} header on every upgrade handshake - the initial connect and each
 * reconnect - so a long-lived WebSocket sender follows token rotation the way the HTTP transport does.
 * The provider is queried at handshake time, not per data frame, because an established WebSocket is
 * not re-authenticated mid-stream. The fixed-token and username/password paths are covered too as a
 * regression guard for the refactor that turned the captured header string into a per-handshake supplier.
 * <p>
 * Each test runs under {@code assertMemoryLeak} so the sender's native buffers are proven freed on close.
 */
public class WebSocketTokenProviderTest {

    @Test
    public void testProviderRequeriedOnEveryReconnect() throws Exception {
        assertMemoryLeak(() -> {
            // The handler ACKs the first frame then drops the connection, forcing the I/O loop to reconnect.
            // The reconnect runs the same buildAndConnect path, so it must re-query the provider and present
            // the next token on the new upgrade - proving refresh-at-handshake, not a token captured once.
            AtomicInteger tokenSeq = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .httpTokenProvider(() -> "TOKEN-" + tokenSeq.incrementAndGet())
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // batch 1 lands, gets ACKed, then the server drops the socket -> reconnect
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();

                    // the reconnect handshake must carry a freshly pulled token (blocks for the reconnect)
                    Assert.assertEquals("Bearer TOKEN-2", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // batch 2 goes through on the new connection, end to end
                    sender.table("foo").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 2, 5_000);
                }
            }
        });
    }

    @Test
    public void testProviderTokenSuppliedOnInitialUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger tokenSeq = new AtomicInteger();
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .httpTokenProvider(() -> "TOKEN-" + tokenSeq.incrementAndGet())
                        .build()) {
                    // the upgrade handshake runs during build(); the provider was queried exactly once for it
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    Assert.assertEquals(1, tokenSeq.get());

                    // sending data must NOT re-query the provider: the established socket carries no new auth
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertEquals(1, tokenSeq.get());
                }
            }
        });
    }

    @Test
    public void testStaticTokenStillSuppliedOverWebSocket() throws Exception {
        assertMemoryLeak(() -> {
            // regression guard for the supplier refactor: a fixed httpToken still reaches the upgrade header
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .httpToken("static-token")
                        .build()) {
                    Assert.assertEquals("Bearer static-token", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                }
            }
        });
    }

    @Test
    public void testThrowingProviderResolvedOncePerConnectRound() throws Exception {
        assertMemoryLeak(() -> {
            // A token-provider failure (a failed silent refresh, or not signed in) is cluster-wide, not a
            // per-endpoint transport fault. The credential is resolved once before the endpoint walk, so the
            // provider is queried exactly once per connect round even across a multi-endpoint failover, and the
            // provider's own error reaches the caller instead of being masked as "all endpoints unreachable".
            AtomicInteger calls = new AtomicInteger();
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // two endpoints at the same reachable server (distinct host strings, so not rejected as
                // duplicates) - a pre-fix per-endpoint pull would query the provider twice for one connect
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + port)
                            .address("127.0.0.1:" + port)
                            .httpTokenProvider(() -> {
                                calls.incrementAndGet();
                                throw new OidcAuthException("no token has been obtained yet; call signIn()");
                            })
                            .build();
                    Assert.fail("expected build() to fail when the token provider throws");
                } catch (OidcAuthException e) {
                    // the provider's own error surfaces directly, not wrapped as a transport failure
                    String msg = e.getMessage();
                    Assert.assertTrue("expected the provider's message, got: " + msg,
                            msg.contains("no token has been obtained yet"));
                    Assert.assertFalse("a provider failure must not be mislabeled as unreachable, got: " + msg,
                            msg.contains("unreachable"));
                } catch (Exception e) {
                    Assert.fail("expected the provider's OidcAuthException to surface, got: " + e);
                }
                // queried once per connect round, not once per endpoint (pre-fix this would be 2)
                Assert.assertEquals(1, calls.get());
            }
        });
    }

    @Test
    public void testThrowingProviderOnReconnectIsRetriedAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            // The riskiest token-provider path: a throw on the BACKGROUND I/O thread during a reconnect. The
            // server ACKs the first frame then drops the socket, forcing a reconnect; on that reconnect the
            // provider throws once (a transient failed silent refresh), then succeeds. connectWithRetry must
            // catch the (non-terminal) throw and retry within the reconnect budget - re-querying the provider -
            // so the sender recovers and batch 2 still lands, rather than the throw killing the I/O thread or
            // being silently swallowed. A regression narrowing that catch (so the throw is not retried) fails here.
            AtomicInteger calls = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            // n==1 initial connect (ok); n==2 first reconnect attempt (transient throw);
                            // n>=3 reconnect retry (ok)
                            if (n == 2) {
                                throw new OidcAuthException("transient: a silent refresh failed");
                            }
                            return "TOKEN-" + n;
                        })
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // batch 1 lands and is ACKed, then the server drops the socket -> background reconnect
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();

                    // the reconnect's first pull threw; connectWithRetry retries, re-querying the provider, and
                    // the retry's token reaches the upgrade (the throwing attempt never connected, so TOKEN-2 is
                    // never seen on the wire)
                    Assert.assertEquals("Bearer TOKEN-3", server.pollAuthorizationHeader(10, TimeUnit.SECONDS));

                    // batch 2 goes through on the recovered connection: the reconnect throw did not terminate it
                    sender.table("foo").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 2, 10_000);
                    Assert.assertTrue("the provider must be re-queried on the reconnect retry (>=3 pulls), got " + calls.get(),
                            calls.get() >= 3);
                }
            }
        });
    }

    @Test
    public void testPersistentlyThrowingProviderOnReconnectTerminatesTheSender() throws Exception {
        assertMemoryLeak(() -> {
            // The complement to the recover-on-transient case: a provider that keeps throwing on every reconnect
            // must NOT be silently swallowed on the background I/O thread - once the (short here) reconnect
            // budget is exhausted the sender terminates, and a subsequent send/flush must surface the failure
            // rather than block forever or silently drop data. A cap on the reconnect duration keeps this fast.
            AtomicInteger calls = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .reconnectMaxDurationMillis(300) // short budget so persistent failure terminates quickly
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            if (n == 1) {
                                return "TOKEN-1"; // initial connect succeeds
                            }
                            throw new OidcAuthException("persistent: not signed in"); // every reconnect pull fails
                        })
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // batch 1 lands and is ACKed, then the server drops the socket -> the reconnect keeps failing
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();

                    // once the reconnect budget exhausts, the terminated sender must surface the failure on a
                    // later send/flush (never a silent success), so drive rows until one throws
                    waitFor(() -> {
                        try {
                            sender.table("foo").longColumn("v", 2L).atNow();
                            sender.flush();
                            return false; // not terminated yet - the reconnect is still within budget
                        } catch (Exception e) {
                            return true; // terminated: the persistent provider failure surfaced, as intended
                        }
                    }, 15_000);
                    Assert.assertTrue("the provider must have been re-queried on the failing reconnect, got " + calls.get(),
                            calls.get() >= 2);
                }
            }
        });
    }

    @Test
    public void testUsernamePasswordStillSuppliedOverWebSocket() throws Exception {
        assertMemoryLeak(() -> {
            // regression guard for the supplier refactor: username/password still becomes the Basic header
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .httpUsernamePassword("user", "pass")
                        .build()) {
                    String expected = "Basic " + Base64.getEncoder().encodeToString(
                            "user:pass".getBytes(StandardCharsets.UTF_8));
                    Assert.assertEquals(expected, server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                }
            }
        });
    }

    // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
    private static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
    }

    private static void waitFor(BoolCondition cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted");
            }
        }
        Assert.fail("waitFor timed out after " + timeoutMillis + "ms");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
    }

    /** ACKs every binary frame so the sender doesn't hang. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * ACKs every binary frame; on the first connection's first frame it closes the socket right after
     * the ACK, so the sender's I/O loop must reconnect to deliver the next batch. Later connections ACK
     * normally.
     */
    private static class DropAfterFirstAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler firstClient;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
                if (totalBinaryReceived.get() == 1) {
                    // brief sleep so the queued ACK flushes before we close the socket under it
                    Thread.sleep(50);
                    client.close();
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
