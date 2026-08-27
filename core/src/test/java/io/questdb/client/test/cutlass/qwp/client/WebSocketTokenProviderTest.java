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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.HandOffCharSequence;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testProviderBufferMutatedDuringTheHandshakeCannotSplice() throws Exception {
        assertMemoryLeak(() -> {
            // Sender.buildWebSocketAuthHeader's supplier applies the same snapshot-before-validate rule as
            // the ILP sender and the query client, and was the one of the three with no test. Without the
            // snapshot validateToken scans the provider's live sequence and the "Bearer " concatenation
            // materialises it again, so a buffer that changes between those two reads ships the mutated
            // bytes - CR/LF included - into the upgrade request.
            final String clean = "GOODTOKEN";
            final String spliced = "abc" + (char) 0x0d + (char) 0x0a + "X-Injected: pwned";
            AtomicInteger pulls = new AtomicInteger();
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .httpTokenProvider(() -> {
                            pulls.incrementAndGet();
                            return new HandOffCharSequence(clean, spliced);
                        })
                        .build()) {
                    Assert.assertNotNull(sender);
                    Assert.assertEquals("the upgrade must carry the bytes that were validated, not a value "
                                    + "swapped in after the scan",
                            "Bearer " + clean, server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    Assert.assertTrue("the provider must have been queried, or this test passes for the "
                            + "wrong reason", pulls.get() >= 1);
                }
            }
        });
    }

    @Test
    public void testCredentialKindTaggedForTheOrphanDrainerTerminalPolicy() throws Exception {
        assertMemoryLeak(() -> {
            // The builder routes a CONSTANT credential through QwpWebSocketSender.fixedAuthHeader
            // and an httpTokenProvider through a bare lambda. That type difference is the whole
            // signal: hasDynamicCredential() reads it, and BackgroundDrainer.connectWithDurableAckRetry
            // decides on it whether a 401 during an orphan drain may quarantine the slot.
            //
            // A mis-tag is silent at build time and asymmetric in cost. Tagging a rotating credential
            // as fixed makes the first 401 of an orphan drain drop a .failed sentinel that nothing in
            // production clears, permanently abandoning replayable rows over a token the next pull
            // would have refreshed. The other direction only delays the operator's signal: a wrong
            // fixed password rides out the attempt threshold and the dwell floor before quarantining.
            //
            // Nothing connected the builder half to the drainer half, so assert both on a real built
            // sender: the header the server actually received (a tag asserted alone would still pass
            // if the credential reached the wire by some other route) and the tag itself, read both
            // directly and through the background reconnect factory the drainer is handed.
            //
            // This is the ONLY test of the classification itself: the drainer's own suites
            // (BackgroundDrainerDurableAckRetryTest, BackgroundDrainerMidDrainAuthRejectTest) stub
            // hasDynamicCredential() on a scripted factory, because what they pin is the terminal policy
            // each verdict produces. Delete this test and both verdicts become assumptions.
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                assertCredentialKind(server, port, "Bearer static-token", false,
                        b -> b.httpToken("static-token"));
                assertCredentialKind(server, port,
                        "Basic " + Base64.getEncoder().encodeToString(
                                "user:pass".getBytes(StandardCharsets.UTF_8)),
                        false,
                        b -> b.httpUsernamePassword("user", "pass"));
                assertCredentialKind(server, port, "Bearer rotating-token", true,
                        b -> b.httpTokenProvider(() -> "rotating-token"));
                // No credential at all: nothing to refresh, so a rejection is never
                // a window a later pull can close.
                assertCredentialKind(server, port, "", false, b -> {
                });
            }
        });
    }

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

    @Test(timeout = 30_000)
    public void testThrowingProviderFailsFastInSyncInitialConnect() throws Exception {
        assertMemoryLeak(() -> {
            // Setting any reconnect_* knob promotes the initial connect to SYNC mode (Sender.build). In SYNC
            // mode a token-provider failure (not signed in / a failed refresh) must STILL fail fast with the
            // provider's own exception - exactly like OFF mode - not be treated as a transport outage and
            // retried for the whole reconnect budget (which would block build() for up to that budget, then
            // surface a transport-shaped wrapper). A deterministic "no token" can never recover by retrying.
            AtomicInteger calls = new AtomicInteger();
            try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                long budgetMillis = 10_000; // if the fix regressed, build() would block ~this long before failing
                long startNanos = System.nanoTime();
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + port)
                            .reconnectMaxDurationMillis(budgetMillis) // -> SYNC initial connect
                            .httpTokenProvider(() -> {
                                calls.incrementAndGet();
                                throw new OidcAuthException("no token has been obtained yet; call signIn()");
                            })
                            .build();
                    Assert.fail("expected build() to fail when the token provider throws");
                } catch (OidcAuthException e) {
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    // the provider's own error, surfaced fast - not a wrapped transport failure after the budget
                    String msg = e.getMessage();
                    Assert.assertTrue("expected the provider's message, got: " + msg,
                            msg.contains("no token has been obtained yet"));
                    Assert.assertTrue("build() must fail fast, not burn the reconnect budget; took " + elapsedMillis + "ms",
                            elapsedMillis < budgetMillis / 2);
                } catch (Exception e) {
                    Assert.fail("SYNC-mode credential failure must surface the provider's OidcAuthException, got: " + e);
                }
                // one deterministic failure, not a budget's worth of retries
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

    @Test(timeout = 60_000)
    public void testCloseBreaksADrainerBlockedInACredentialPull() throws Exception {
        assertMemoryLeak(() -> {
            // The reconnect walk publishes the WebSocketClient it is about to block on so close() can break it
            // (ConnectCancellation), but the credential pull that now precedes the walk is caller code owning
            // no socket, so closeTraffic() cannot reach it. A pull can outlast close()'s 30s shutdown budget -
            // OidcDeviceAuth.getToken() waits up to 6 x httpTimeoutMillis behind a peer's silent refresh - and
            // during an IdP outage the drainer sits inside a pull for most of every retry cycle, so close()
            // lands there routinely. Before the fix close() burned the whole budget and then threw
            // "cursor I/O thread did not stop", delegating teardown, on what is a clean shutdown.
            CountDownLatch pullEntered = new CountDownLatch(1);
            CountDownLatch neverReleased = new CountDownLatch(1);
            AtomicBoolean blockNextPull = new AtomicBoolean(false);
            AtomicBoolean sawInterrupt = new AtomicBoolean(false);
            AtomicInteger calls = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            if (blockNextPull.get()) {
                                pullEntered.countDown();
                                try {
                                    // only an interrupt can free this, exactly like OidcDeviceAuth's timed
                                    // wait for its instance lock behind a peer's silent refresh
                                    neverReleased.await(45, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    sawInterrupt.set(true);
                                    throw new OidcAuthException("interrupted while waiting for a token");
                                }
                            }
                            return "TOKEN-" + n;
                        })
                        .build();
                boolean closed = false;
                try {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // arm the block, then let the server's drop drive the background reconnect into the pull
                    blockNextPull.set(true);
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertTrue("the drainer must reach the credential pull",
                            pullEntered.await(15, TimeUnit.SECONDS));

                    long startNanos = System.nanoTime();
                    sender.close();
                    closed = true;
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    // the budget is 30s; anything near it means close() waited it out instead of breaking
                    // the pull. A generous ceiling keeps this off the CI flake line while still failing the
                    // pre-fix behaviour by a wide margin.
                    Assert.assertTrue("close() must break the pull, not wait out the shutdown budget; took "
                            + elapsedMillis + "ms", elapsedMillis < 15_000);
                    Assert.assertTrue("close() must interrupt the thread parked in the pull", sawInterrupt.get());
                } finally {
                    if (!closed) {
                        neverReleased.countDown();
                        sender.close();
                    }
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testPersistentCredentialOutageIsReportedToTheErrorHandler() throws Exception {
        assertMemoryLeak(() -> {
            // Retrying a credential outage forever (Invariant B) must not make it programmatically INVISIBLE.
            // A revoked refresh token or a permanently dead IdP is not self-healing, yet the drainer keeps
            // retrying and flush() keeps returning success while SF absorbs the rows; without a dispatched
            // SenderError the only signal is a throttled slf4j WARN - and this library ships embedded, often
            // with no binding configured - until SF fills and the failure resurfaces as ring backpressure,
            // pointing the operator at disk sizing instead of at their credentials. The auth/upgrade and
            // durable-ack policy failures already dispatch a RETRIABLE error for exactly this reason; the
            // credential arm did not. RETRIABLE, not TERMINAL: the handler learns the wire is down while the
            // producer stays alive and no data is at risk.
            AtomicBoolean providerFailing = new AtomicBoolean(false);
            AtomicInteger calls = new AtomicInteger();
            AtomicReference<SenderError> credentialError = new AtomicReference<>();
            AtomicReference<SenderError> terminalError = new AtomicReference<>();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .errorHandler(e -> {
                            if (e.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                                terminalError.compareAndSet(null, e);
                            } else if (e.getServerMessage() != null
                                    && e.getServerMessage().contains("credential-unavailable")) {
                                credentialError.compareAndSet(null, e);
                            }
                        })
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            if (providerFailing.get()) {
                                throw new OidcAuthException("persistent: not signed in");
                            }
                            return "TOKEN-" + n;
                        })
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // arm the outage before the drop, so every reconnect pull throws
                    providerFailing.set(true);
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                    // the handler must be told, by category and by message, that the CREDENTIAL is the problem
                    waitFor(() -> credentialError.get() != null, 15_000);
                    SenderError err = credentialError.get();
                    Assert.assertEquals(SenderError.Category.SECURITY_ERROR, err.getCategory());
                    Assert.assertEquals(SenderError.Policy.RETRIABLE, err.getAppliedPolicy());
                    Assert.assertTrue("the provider's own message must reach the handler: " + err.getServerMessage(),
                            err.getServerMessage().contains("not signed in"));

                    // and it stays RETRIABLE: no terminal, and the producer is still alive
                    Assert.assertNull("a credential outage must never latch a terminal", terminalError.get());
                    sender.table("foo").longColumn("v", 2L).atNow();

                    // the provider recovers -> the next reconnect succeeds and the buffered rows drain
                    providerFailing.set(false);
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 2, 15_000);
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testPersistentlyThrowingProviderOnReconnectDoesNotTerminateAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            // Invariant B: the RUNNING store-and-forward drainer must NEVER terminate on a token-provider
            // failure, however long it persists. A failing provider (IdP unreachable, a silent refresh failing,
            // sign-in not yet complete) is a transient outage like any other - the un-acked rows stay safe in SF
            // and the sender recovers once a token is available again. Here the provider throws for FAR longer
            // than the (deliberately short) reconnect budget: the sender must stay alive the whole time - no
            // terminal, no exception surfaced to the producer - and then ship the buffered row once the provider
            // recovers. Before the fix the drainer latched a TERMINAL SECURITY_ERROR at reconnectMaxDurationMillis
            // and dropped the producer store-and-forward had promised to keep alive.
            AtomicBoolean providerFailing = new AtomicBoolean(false);
            AtomicInteger calls = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                long budgetMillis = 300; // SHORT: the outage below far exceeds it, proving the budget is not consulted
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .reconnectMaxDurationMillis(budgetMillis)
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            if (providerFailing.get()) {
                                throw new OidcAuthException("persistent: not signed in");
                            }
                            return "TOKEN-" + n;
                        })
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // Arm the provider failure on the established connection, BEFORE the drop triggers a reconnect,
                    // so every reconnect pull throws (no race where the first reconnect succeeds first).
                    providerFailing.set(true);
                    // batch 1 lands and is ACKed on the initial connection, then the server drops the socket ->
                    // the background reconnect loop starts and every provider pull now throws
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                    // let the failing reconnect run for 4x the budget - the old code would have terminated at 1x
                    int callsAtOutageStart = calls.get();
                    Thread.sleep(budgetMillis * 4);

                    // the drainer kept re-querying the provider (retrying, not giving up) ...
                    Assert.assertTrue("the provider must be re-queried during the outage, got " + calls.get(),
                            calls.get() > callsAtOutageStart);
                    // ... and the sender is still ALIVE: buffering another row must not surface a terminal even
                    // though every reconnect is currently failing. Before the fix this threw a SECURITY_ERROR
                    // "token-provider-failed" once the budget elapsed.
                    try {
                        sender.table("foo").longColumn("v", 2L).atNow();
                    } catch (Exception e) {
                        Assert.fail("the drainer terminated the sender on a transient provider outage: " + e.getMessage());
                    }

                    // the provider recovers -> the next reconnect succeeds and the buffered row drains
                    providerFailing.set(false);
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 2, 15_000);
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testRepeated401sWithDynamicTokenDoNotTerminateLiveSenderAndRecover() throws Exception {
        assertMemoryLeak(() -> {
            // This composes the production foreground path all the way from a dynamic HTTP token provider,
            // through real 401 upgrade responses, to the live store-and-forward engine. Unit tests of the
            // orphan drainer's dynamic-credential policy cannot catch the running sender accidentally adopting
            // the orphan-only quarantine policy: that would latch TERMINAL/DATA_LOSS, drop .failed, and kill the
            // producer once the ordinary reconnect budget elapsed.
            final long budgetMillis = 300;
            final String senderId = "live-dynamic-401";
            AtomicInteger tokenPulls = new AtomicInteger();
            AtomicReference<SenderError> terminalOrDataLoss = new AtomicReference<>();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            Path sfDir = temp.newFolder("live-dynamic-401-sf").toPath();
            Path failedSentinel = sfDir.resolve(senderId).resolve(".failed");

            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .storeAndForwardDir(sfDir.toString())
                        .senderId(senderId)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .reconnectMaxDurationMillis(budgetMillis)
                        .errorHandler(error -> {
                            if (error.getAppliedPolicy() == SenderError.Policy.TERMINAL
                                    || error.getCategory() == SenderError.Category.DATA_LOSS) {
                                terminalOrDataLoss.compareAndSet(null, error);
                            }
                        })
                        .httpTokenProvider(() -> "TOKEN-" + tokenPulls.incrementAndGet())
                        .build()) {
                    try {
                        Assert.assertEquals("Bearer TOKEN-1",
                                server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                        // The existing connection still accepts and ACKs batch 1, then drops. Every reconnect
                        // from that point receives a genuine HTTP 401 after pulling a fresh token.
                        server.setRejectWithStatus(401, "Unauthorized");
                        sender.table("foo").longColumn("v", 1L).atNow();
                        sender.flush();
                        waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                        int pullsAtOutageStart = tokenPulls.get();
                        Thread.sleep(budgetMillis * 4);
                        Assert.assertTrue("the live sender must keep retrying 401s beyond its reconnect budget",
                                tokenPulls.get() > pullsAtOutageStart);
                        Assert.assertNull("dynamic-token 401s must not become TERMINAL or DATA_LOSS",
                                terminalOrDataLoss.get());
                        Assert.assertFalse("a live dynamic-token outage must not quarantine the active slot",
                                Files.exists(failedSentinel));

                        // A producer call made while the 401 outage is still active must remain usable. Once the
                        // server accepts the next rotated token, this buffered batch must drain normally.
                        sender.table("foo").longColumn("v", 2L).atNow();
                        sender.flush();
                        Assert.assertNull("the producer must survive the extended 401 outage",
                                terminalOrDataLoss.get());

                        server.setRejectWithStatus(0, null);
                        waitFor(() -> handler.totalBinaryReceived.get() >= 2, 15_000);
                        Assert.assertNull("recovery must not leave a terminal or data-loss report",
                                terminalOrDataLoss.get());
                        Assert.assertFalse("recovery must leave no .failed sentinel",
                                Files.exists(failedSentinel));
                    } finally {
                        // Let sender.close() reconnect and finish its cleanup even when an assertion above fails.
                        server.setRejectWithStatus(0, null);
                    }
                }

                Assert.assertNull("close must not synthesize a terminal or data-loss report",
                        terminalOrDataLoss.get());
                Assert.assertFalse("the recovered sender must close without a .failed sentinel",
                        Files.exists(failedSentinel));
            }
        });
    }

    @Test(timeout = 60_000)
    public void testCredentialFailuresInterleavedWithRoleRejectsDoNotTerminateAndRecover() throws Exception {
        assertMemoryLeak(() -> {
            // Neither a token-provider failure nor a transient role reject may terminate the running drainer, and
            // interleaving them must not either: both fall through to capped backoff and retry indefinitely
            // (Invariant B). Here credential blips (even provider calls throw) alternate with 421 role rejects
            // (odd calls return a token whose upgrade the server rejects) for far longer than the reconnect
            // budget; the sender must survive the whole span and then ship batch 2 once both faults clear. Before
            // the fix the credential blips accumulated to a budget-latched terminal and the sender was dropped.
            AtomicInteger calls = new AtomicInteger();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                long budgetMillis = 300; // short: the interleaved outage below far exceeds it
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + port)
                        .reconnectInitialBackoffMillis(20)
                        .reconnectMaxBackoffMillis(20)
                        .reconnectMaxDurationMillis(budgetMillis)
                        .httpTokenProvider(() -> {
                            int n = calls.incrementAndGet();
                            // call 1: the initial connect (must succeed). Then alternate on every reconnect
                            // attempt: even calls THROW (a credential blip), odd calls RETURN a token whose
                            // connect then hits the 421 role reject below. So credential failures and role
                            // rejects strictly alternate across the whole outage.
                            if (n > 1 && n % 2 == 0) {
                                throw new OidcAuthException("transient: a silent refresh failed");
                            }
                            return "TOKEN-" + n;
                        })
                        .build()) {
                    Assert.assertEquals("Bearer TOKEN-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));

                    // reject every NEW handshake with a transient 421 role reject BEFORE the drop, so a
                    // token-returning reconnect attempt deterministically hits it. The already-established
                    // initial connection is unaffected and still ships batch 1.
                    server.setRejectWithRole("replica");
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                    // let the interleaved credential + role failures run for well over the budget; the sender
                    // must NOT terminate on either fault class or their interleaving
                    Thread.sleep(budgetMillis * 4);
                    try {
                        sender.table("foo").longColumn("v", 2L).atNow();
                    } catch (Exception e) {
                        Assert.fail("the drainer terminated during interleaved credential/role failures: " + e.getMessage());
                    }

                    // clear the reject: the next token-returning reconnect now succeeds and batch 2 drains
                    server.setRejectWithRole(null);
                    sender.flush();
                    waitFor(() -> handler.totalBinaryReceived.get() >= 2, 15_000);
                    Assert.assertTrue("the provider must have been re-queried across the reconnect phase, got " + calls.get(),
                            calls.get() >= 4);
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

    private static void assertCredentialKind(
            TestWebSocketServer server,
            int port,
            String expectedHeader,
            boolean expectedDynamic,
            Consumer<Sender.LineSenderBuilder> credential
    ) throws Exception {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + port);
        credential.accept(builder);
        try (Sender sender = builder.build()) {
            Assert.assertEquals("the configured credential must reach the upgrade header",
                    expectedHeader, server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
            QwpWebSocketSender qwp = (QwpWebSocketSender) sender;
            Assert.assertEquals("credential tag for [" + expectedHeader + "]",
                    expectedDynamic, qwp.isCredentialDynamic());
            // The value BackgroundDrainer.connectWithDurableAckRetry actually reads:
            // ReconnectFactory.hasDynamicCredential() on the background factory an
            // orphan drainer is handed.
            Assert.assertEquals("drainer-visible credential tag for [" + expectedHeader + "]",
                    expectedDynamic,
                    qwp.newBackgroundReconnectFactory(() -> false).hasDynamicCredential());
        }
    }
}
