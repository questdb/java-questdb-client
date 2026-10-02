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
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.test.cutlass.auth.TokenTestKit.FakeClock;
import io.questdb.client.test.cutlass.auth.TokenTestKit.ManualScheduler;
import io.questdb.client.test.cutlass.auth.TokenTestKit.ScriptedSource;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance tests C9-C16 and C23 of the dynamic-credential specification (design/qwp-token-provider-spec.md,
 * section 10) for the WebSocket ingest {@code Sender}: a {@link RefreshingTokenProvider} wired into real
 * connect, reconnect, failover, store-and-forward and orphan-drain paths, against a server that validates the
 * presented token rather than rejecting blindly.
 */
public class WebSocketDynamicCredentialTest {
    private static final long HOUR = 3_600_000L;
    private static final long T0 = 1_800_000_000_000L;

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testAsyncInitializationRetriesProviderFailuresOfEitherClassification() throws Exception {
        // C12, async rows: a credential-unavailable failure during async initialization is retried indefinitely
        // whatever its classification, reported to the error handler, and recovers with the source.
        for (boolean retryable : new boolean[]{true, false}) {
            assertMemoryLeak(() -> {
                ScriptedSource source = new ScriptedSource()
                        .thenThrow(new TokenUnavailableException("IdP says no", retryable));
                ErrorCollector errors = new ErrorCollector();
                AckHandler handler = new AckHandler();
                try (TestWebSocketServer server = startServer(handler);
                     RefreshingTokenProvider provider = fastProvider(source).build();
                     Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                             .address("localhost:" + server.getPort())
                             .initialConnectMode(Sender.InitialConnectMode.ASYNC)
                             .reconnectInitialBackoffMillis(10)
                             .reconnectMaxBackoffMillis(20)
                             .errorHandler(errors)
                             .httpTokenProvider(provider)
                             .build()) {
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush();
                    await(() -> errors.count("credential-unavailable") >= 2, 10_000,
                            "the async initial connect to report and retry the provider failure");
                    Assert.assertEquals("retried, never terminal [retryable=" + retryable + ']',
                            0, errors.terminalCount());
                    source.then(() -> new ExpiringToken("RECOVERED", System.currentTimeMillis() + HOUR));
                    await(() -> handler.frames.get() >= 1, 10_000, "the row to arrive once the source recovers");
                    Assert.assertEquals("Bearer RECOVERED", lastHeader(server));
                }
            });
        }
    }

    @Test(timeout = 30_000)
    public void testChallengeInsufficientScopeDoesNotRetry() throws Exception {
        // C23: a 401 whose Bearer challenge carries an error other than invalid_token does not trigger the retry.
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            try (TestWebSocketServer server = startServer(new AckHandler());
                 RefreshingTokenProvider provider = fastProvider(source).build()) {
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 401 : 0);
                server.setRejectWwwAuthenticate("Bearer realm=\"questdb\", error=\"insufficient_scope\"");
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .httpTokenProvider(provider)
                            .build()
                            .close();
                    Assert.fail("a 401 with error=insufficient_scope must not be retried");
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(401, e.getStatusCode());
                    Assert.assertEquals("insufficient_scope", e.getBearerError());
                    Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("auth-rejected"));
                }
                Assert.assertEquals("exactly one upgrade", 1, server.upgradeRequestCount());
                Assert.assertEquals("no forced refresh", 1, source.calls());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testChallengeInvalidTokenRetries() throws Exception {
        // C23: a 401 with Bearer error="invalid_token" triggers the retry.
        assertChallengeRetries("Bearer realm=\"questdb\", error=\"invalid_token\", error_description=\"expired\"");
    }

    @Test(timeout = 30_000)
    public void testChallengeOtherSchemeOnlyRetries() throws Exception {
        // C23: without a Bearer challenge carrying an error, the status code alone decides.
        assertChallengeRetries("Basic realm=\"questdb\"");
    }

    @Test(timeout = 30_000)
    public void testChallengePlain401Retries() throws Exception {
        // C23: a plain 401, with no challenge at all, triggers the retry.
        assertChallengeRetries(null);
    }

    @Test(timeout = 60_000)
    public void testFailoverAcrossARotationSendsTheNewTokenAndReplaysOnce() throws Exception {
        // C14: connected to A with T1, the token rotates, A dies. B receives the NEW token, and the frames A
        // never acknowledged are replayed to B exactly once.
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            CollectingHandler silentA = new CollectingHandler(false);
            CollectingHandler ackB = new CollectingHandler(true);
            try (RefreshingTokenProvider provider = fastProvider(source).forcedMinIntervalMillis(0).build();
                 TestWebSocketServer b = startServer(ackB)) {
                b.setAuthorizationValidator(h -> "Bearer T2".equals(h) ? 0 : 401);
                TestWebSocketServer a = startServer(silentA);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + a.getPort())
                        .address("localhost:" + b.getPort())
                        .httpTokenProvider(provider)
                        .build()) {
                    Assert.assertEquals("Bearer T1", a.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    for (int i = 0; i < 5; i++) {
                        sender.table("t").longColumn("v", i).atNow();
                        sender.flush();
                    }
                    await(() -> silentA.frames.get() == 5, 10_000, "A to receive the unacknowledged frames");

                    // rotate: the provider now holds T2
                    provider.onTokenRejected("T1", 401);
                    Assert.assertEquals("T2", provider.getToken().toString());

                    a.close(); // A dies; the reconnect fails over to B
                    await(() -> ackB.distinctPayloads.size() == 5, 15_000, "the unacked frames to replay at B");
                    Assert.assertEquals("B must receive the new token", "Bearer T2",
                            b.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    Assert.assertEquals("B must never see the old token", 0, b.authRejectCount());
                    Assert.assertEquals("each unacked frame replayed exactly once", 5, ackB.frames.get());
                } finally {
                    a.close();
                }
                Assert.assertEquals(silentA.payloads, ackB.payloads);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testForbiddenNeverTriggersAForcedRefresh() throws Exception {
        // D2: a 403 is an authorization decision; it gets no forced refresh and no retry.
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            try (TestWebSocketServer server = startServer(new AckHandler());
                 RefreshingTokenProvider provider = fastProvider(source).build()) {
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 403 : 0);
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .httpTokenProvider(provider)
                            .build()
                            .close();
                    Assert.fail("a 403 must fail startup");
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(403, e.getStatusCode());
                }
                Assert.assertEquals(1, server.upgradeRequestCount());
                Assert.assertEquals("a 403 must not force a refresh", 1, source.calls());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testOrphanDrainAcrossARotationDrainsWithTheNewToken() throws Exception {
        // C16: an orphan drain whose first upgrade presents the stale token gets one 401, refreshes, and drains
        // the whole slot with the new token. No quarantine.
        assertMemoryLeak(() -> {
            String sfDir = temp.newFolder("orphan-rotation").getAbsolutePath();
            final int frames = 20;
            try (TestWebSocketServer silent = startServer(new CollectingHandler(false))) {
                String ghostCfg = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                        + ";sender_id=ghost;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    for (int i = 0; i < frames; i++) {
                        ghost.table("t").longColumn("v", i).atNow();
                        ghost.flush();
                    }
                }
            }
            Assert.assertEquals(1, OrphanScanner.scan(sfDir, "primary").size());

            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            CollectingHandler ack = new CollectingHandler(true);
            AtomicInteger t1Uses = new AtomicInteger();
            try (RefreshingTokenProvider provider = fastProvider(source).forcedMinIntervalMillis(0).build();
                 TestWebSocketServer server = startServer(ack)) {
                // T1 is good for exactly one upgrade - the foreground's - and is revoked after it. The drainer's
                // first upgrade presents the cached T1 and must recover through the one-retry rule.
                server.setAuthorizationValidator(h -> {
                    if ("Bearer T2".equals(h)) {
                        return 0;
                    }
                    return "Bearer T1".equals(h) && t1Uses.incrementAndGet() == 1 ? 0 : 401;
                });
                try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .storeAndForwardDir(sfDir)
                        .senderId("primary")
                        .drainOrphans(true)
                        .httpTokenProvider(provider)
                        .build()) {
                    await(() -> ack.distinctPayloads.size() == frames, 15_000, "the orphan slot to drain");
                }
                Assert.assertEquals("the drainer met the stale token exactly once", 1, server.authRejectCount());
                Assert.assertEquals("the drain used the new token", 2, source.calls());
            }
            Assert.assertFalse("a slot that drained must not be quarantined",
                    Files.exists(new java.io.File(sfDir, "ghost/" + OrphanScanner.FAILED_SENTINEL_NAME).toPath()));
        });
    }

    @Test(timeout = 30_000)
    public void testPersistent401AfterTheRetryFailsStartup() throws Exception {
        // C11, initialization rows: a 401 that survives the one retry fails startup, in OFF and in SYNC mode
        // (SYNC does not spend its reconnect budget on it).
        for (Sender.InitialConnectMode mode : new Sender.InitialConnectMode[]{
                Sender.InitialConnectMode.OFF, Sender.InitialConnectMode.SYNC}) {
            assertMemoryLeak(() -> {
                ScriptedSource source = new ScriptedSource()
                        .thenToken("T1", System.currentTimeMillis() + HOUR)
                        .thenToken("T2", System.currentTimeMillis() + HOUR);
                try (TestWebSocketServer server = startServer(new AckHandler());
                     RefreshingTokenProvider provider = fastProvider(source).build()) {
                    server.setAuthorizationValidator(h -> 401);
                    long start = System.nanoTime();
                    try {
                        Sender.builder(Sender.Transport.WEBSOCKET)
                                .address("localhost:" + server.getPort())
                                .initialConnectMode(mode)
                                .reconnectMaxDurationMillis(20_000)
                                .httpTokenProvider(provider)
                                .build()
                                .close();
                        Assert.fail("a persistent 401 must fail startup [mode=" + mode + ']');
                    } catch (QwpAuthFailedException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("auth-rejected"));
                    }
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    Assert.assertTrue("startup must fail fast, took " + elapsedMillis + " ms", elapsedMillis < 10_000);
                    Assert.assertEquals("the original attempt plus one retry with the refreshed token [mode=" + mode + ']',
                            2, server.authRejectCount());
                    Assert.assertEquals(Arrays.asList("Bearer T1", "Bearer T2"), drainHeaders(server));
                }
            });
        }
    }

    @Test(timeout = 60_000)
    public void testPersistent401WhileEstablishedIsRetriedAndReported() throws Exception {
        // C11, established row: a 401 that survives the retry is retried indefinitely and reported as a
        // retriable security error that names the failure class (decision D1).
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource().then(
                    () -> new ExpiringToken("T-" + System.nanoTime(), System.currentTimeMillis() + HOUR));
            ErrorCollector errors = new ErrorCollector();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            AtomicInteger accepted = new AtomicInteger();
            try (TestWebSocketServer server = startServer(handler);
                 RefreshingTokenProvider provider = fastProvider(source).forcedMinIntervalMillis(0).build()) {
                server.setAuthorizationValidator(h -> accepted.getAndIncrement() == 0 ? 0 : 401);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(20)
                        .errorHandler(errors)
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush();
                    await(() -> errors.count("auth-rejected") >= 3, 15_000, "repeated auth-rejected reports");
                    Assert.assertEquals("an established sender never goes terminal on a 401", 0, errors.terminalCount());
                    for (SenderError e : errors.errors) {
                        if (e.getServerMessage().contains("auth-rejected")) {
                            Assert.assertEquals(SenderError.Category.SECURITY_ERROR, e.getCategory());
                            Assert.assertEquals(SenderError.Policy.RETRIABLE, e.getAppliedPolicy());
                        }
                    }
                    server.setAuthorizationValidator(null);
                    sender.table("t").longColumn("v", 2).atNow();
                    sender.flush();
                    await(() -> handler.frames.get() >= 2, 15_000, "recovery once the server accepts again");
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testProviderOutageWhileEstablishedIsRetriedReportedAndRecovers() throws Exception {
        // C13: the source fails while a connected sender's token expires. The reconnect cannot get a credential;
        // it is retried indefinitely and reported, and recovers when the source does.
        assertMemoryLeak(() -> {
            SkewedClock clock = new SkewedClock();
            ScriptedSource source = new ScriptedSource().thenToken("T1", System.currentTimeMillis() + HOUR);
            ErrorCollector errors = new ErrorCollector();
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = startServer(handler);
                 RefreshingTokenProvider provider = fastProvider(source).clock(clock).build();
                 Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                         .address("localhost:" + server.getPort())
                         .reconnectInitialBackoffMillis(10)
                         .reconnectMaxBackoffMillis(20)
                         .errorHandler(errors)
                         .httpTokenProvider(provider)
                         .build()) {
                Assert.assertEquals("Bearer T1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                // the IdP goes down, and the held token expires (the clock jumps past it)
                source.thenThrow(TokenUnavailableException.retryable("IdP unreachable"));
                clock.skewMillis = 2 * HOUR;
                sender.table("t").longColumn("v", 1).atNow();
                sender.flush(); // ACKed on the live connection, which the server then drops
                await(() -> errors.count("credential-unavailable") >= 3, 15_000,
                        "repeated credential-unavailable reports");
                Assert.assertEquals(0, errors.terminalCount());
                for (SenderError e : errors.errors) {
                    if (e.getServerMessage().contains("credential-unavailable")) {
                        Assert.assertEquals(SenderError.Policy.RETRIABLE, e.getAppliedPolicy());
                        Assert.assertTrue(e.getServerMessage(), e.getServerMessage().contains("IdP unreachable"));
                    }
                }
                sender.table("t").longColumn("v", 2).atNow();
                sender.flush(); // buffered in store-and-forward meanwhile
                source.then(() -> new ExpiringToken("T2", clock.wallClockMillis() + HOUR));
                await(() -> handler.frames.get() >= 2, 15_000, "the buffered row to arrive after recovery");
                Assert.assertEquals("Bearer T2", lastHeader(server));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testReconnectAfterExpiryCarriesTheRefreshedToken() throws Exception {
        // C9: the server rejects expired tokens against a clock shared with the provider. The proactive refresh
        // rotated the token before the old one expired, so the reconnect carries the new one and sees no 401.
        assertMemoryLeak(() -> {
            FakeClock clock = new FakeClock(T0);
            ManualScheduler scheduler = new ManualScheduler(clock);
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1." + (T0 + HOUR), T0 + HOUR)
                    .thenToken("T2." + (T0 + 2 * HOUR), T0 + 2 * HOUR);
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            try (TestWebSocketServer server = startServer(handler);
                 RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                         .clock(clock).scheduler(scheduler).random(() -> 0.5).build()) {
                scheduler.runNext(); // the prefetch: T1
                server.setAuthorizationValidator(h -> expiredAt(h) <= clock.wallClockMillis() ? 401 : 0);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .httpTokenProvider(provider)
                        .build()) {
                    Assert.assertTrue(server.pollAuthorizationHeader(5, TimeUnit.SECONDS).startsWith("Bearer T1."));
                    scheduler.advanceAndRunNext(); // the proactive refresh at the half-life: T2
                    clock.advanceMillis(HOUR / 2 + 60_000); // T1 is now expired at the server
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush(); // ACKed, then the server drops the connection
                    String reconnect = server.pollAuthorizationHeader(5, TimeUnit.SECONDS);
                    Assert.assertNotNull("the sender must reconnect", reconnect);
                    Assert.assertTrue(reconnect, reconnect.startsWith("Bearer T2."));
                    sender.table("t").longColumn("v", 2).atNow();
                    sender.flush();
                    await(() -> handler.frames.get() >= 2, 10_000, "the second row");
                }
                Assert.assertEquals("no 401 may be observed", 0, server.authRejectCount());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testStaleTokenGetsExactlyOne401ThenAnImmediateRetry() throws Exception {
        // C10: the reconnect presents a token the server has revoked. Exactly one 401, then an immediate retry of
        // the same endpoint with the refreshed token: no backoff, no failed-attempt event, no AUTH_FAILED.
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            DropAfterFirstAckHandler handler = new DropAfterFirstAckHandler();
            List<SenderConnectionEvent> events = new CopyOnWriteArrayList<>();
            Set<String> revoked = Collections.synchronizedSet(new HashSet<>());
            try (TestWebSocketServer server = startServer(handler);
                 RefreshingTokenProvider provider = fastProvider(source).build()) {
                server.setAuthorizationValidator(h -> revoked.contains(h) ? 401 : 0);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .initialConnectMode(Sender.InitialConnectMode.OFF)
                        // a retry through the reconnect loop would wait at least this long
                        .reconnectInitialBackoffMillis(5_000)
                        .reconnectMaxBackoffMillis(5_000)
                        .connectionListener(events::add)
                        .httpTokenProvider(provider)
                        .build()) {
                    Assert.assertEquals("Bearer T1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    revoked.add("Bearer T1");
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush(); // ACKed, then the server drops the connection
                    Assert.assertEquals("the reconnect first presents the cached token",
                            "Bearer T1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    long rejectedAt = System.nanoTime();
                    Assert.assertEquals("then retries with the refreshed one",
                            "Bearer T2", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    long retryMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rejectedAt);
                    Assert.assertTrue("the retry must be immediate, not after backoff; took " + retryMillis + " ms",
                            retryMillis < 4_000);
                    sender.table("t").longColumn("v", 2).atNow();
                    sender.flush();
                    await(() -> handler.frames.get() >= 2, 10_000, "the second row");
                    await(() -> kinds(events).contains(SenderConnectionEvent.Kind.RECONNECTED), 5_000, "RECONNECTED");
                }
                Assert.assertEquals("exactly one 401", 1, server.authRejectCount());
                List<SenderConnectionEvent.Kind> kinds = kinds(events);
                Assert.assertFalse("only the round's final outcome is reported: " + kinds,
                        kinds.contains(SenderConnectionEvent.Kind.AUTH_FAILED));
                Assert.assertFalse("the retried 401 is not an endpoint failure: " + kinds,
                        kinds.contains(SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED));
            }
        });
    }

    @Test(timeout = 60_000)
    public void testSentinelTokensNeverLeakFromTheClient() throws Exception {
        // C20 at the client level: across a stale-token 401 and its retry (C10), a persistent 401 (C11), a cold
        // provider failure (C4) and a malformed source result, the tokens never appear in any log line (every
        // logger, every level), exception, SenderError, connection event, health snapshot or toString().
        final String stale = "SENTINEL-STALE-" + System.nanoTime();
        final String fresh = "SENTINEL-FRESH-" + System.nanoTime();
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        ch.qos.logback.classic.Level savedLevel = root.getLevel();
        root.setLevel(ch.qos.logback.classic.Level.ALL);
        root.addAppender(appender);
        List<String> rendered = new CopyOnWriteArrayList<>();
        try {
            assertMemoryLeak(() -> {
                ErrorCollector errors = new ErrorCollector();
                List<SenderConnectionEvent> events = new CopyOnWriteArrayList<>();
                // C10: the stale token is rejected once, the fresh one is accepted
                ScriptedSource source = new ScriptedSource()
                        .thenToken(stale, System.currentTimeMillis() + HOUR)
                        .thenToken(fresh, System.currentTimeMillis() + HOUR);
                try (TestWebSocketServer server = startServer(new AckHandler());
                     RefreshingTokenProvider provider = fastProvider(source).build()) {
                    server.setAuthorizationValidator(h -> h.contains(stale) ? 401 : 0);
                    try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .errorHandler(errors)
                            .connectionListener(events::add)
                            .httpTokenProvider(provider)
                            .build()) {
                        sender.table("t").longColumn("v", 1).atNow();
                        sender.flush();
                        rendered.add(sender.health().toString());
                        rendered.add(sender.toString());
                    }
                    rendered.add(provider.toString());

                    // C11: every token is rejected
                    server.setAuthorizationValidator(h -> 401);
                    try {
                        Sender.builder(Sender.Transport.WEBSOCKET)
                                .address("localhost:" + server.getPort())
                                .httpTokenProvider(provider)
                                .build()
                                .close();
                        Assert.fail();
                    } catch (RuntimeException e) {
                        renderThrowable(e, rendered);
                    }
                }
                // C4 and a malformed result: the source fails, then returns an already-expired token
                ScriptedSource failing = new ScriptedSource()
                        .thenThrow(TokenUnavailableException.retryable("IdP unreachable"))
                        .thenToken(stale, 1L);
                try (TestWebSocketServer server = startServer(new AckHandler());
                     RefreshingTokenProvider provider = fastProvider(failing).build()) {
                    try {
                        Sender.builder(Sender.Transport.WEBSOCKET)
                                .address("localhost:" + server.getPort())
                                .httpTokenProvider(provider)
                                .build()
                                .close();
                        Assert.fail();
                    } catch (RuntimeException e) {
                        renderThrowable(e, rendered);
                    }
                    rendered.add(provider.toString());
                    rendered.add(String.valueOf(provider.getLastFailure()));
                }
                for (SenderError e : errors.errors) {
                    rendered.add(e.toString());
                    rendered.add(String.valueOf(e.getServerMessage()));
                }
                for (SenderConnectionEvent e : events) {
                    rendered.add(e.toString());
                    if (e.getCause() != null) {
                        renderThrowable(e.getCause(), rendered);
                    }
                }
            });
        } finally {
            root.detachAppender(appender);
            root.setLevel(savedLevel);
            appender.stop();
        }
        for (ch.qos.logback.classic.spi.ILoggingEvent event : appender.list) {
            rendered.add(event.getFormattedMessage());
            for (ch.qos.logback.classic.spi.IThrowableProxy t = event.getThrowableProxy(); t != null; t = t.getCause()) {
                rendered.add(t.getMessage());
            }
        }
        Assert.assertTrue("the scenarios must have produced output to inspect", rendered.size() > 20);
        for (String s : rendered) {
            if (s != null) {
                Assert.assertFalse("a token leaked into: " + s, s.contains(stale) || s.contains(fresh));
            }
        }
    }

    @Test(timeout = 30_000)
    public void testStaticCredentialNeverGetsTheRetry() throws Exception {
        // section 8.2: a static credential never gets the retry - presenting the same bytes again cannot help.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer(new AckHandler())) {
                server.setAuthorizationValidator(h -> 401);
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .httpToken("static")
                            .build()
                            .close();
                    Assert.fail();
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(401, e.getStatusCode());
                }
                Assert.assertEquals(1, server.upgradeRequestCount());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testStoreAndForwardAcrossARotationDeliversEveryRow() throws Exception {
        // C15: the server rejects T1 with 401 while the IdP still hands out T1; the producer keeps writing into
        // store-and-forward. Once the IdP rotates, every row is delivered: no quarantine, no data loss, no terminal.
        assertMemoryLeak(() -> {
            String sfDir = temp.newFolder("sf-rotation").getAbsolutePath();
            ScriptedSource source = new ScriptedSource().thenToken("T1", System.currentTimeMillis() + HOUR);
            ErrorCollector errors = new ErrorCollector();
            CollectingHandler ack = new CollectingHandler(true);
            AtomicInteger connections = new AtomicInteger();
            try (TestWebSocketServer server = startServer(ack);
                 RefreshingTokenProvider provider = fastProvider(source).forcedMinIntervalMillis(0).build()) {
                // T1 is accepted for the first connection only
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) && connections.getAndIncrement() > 0 ? 401 : 0);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .storeAndForwardDir(sfDir)
                        .senderId("rot")
                        .reconnectInitialBackoffMillis(10)
                        .reconnectMaxBackoffMillis(50)
                        .errorHandler(errors)
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 0).atNow();
                    sender.flush();
                    await(() -> ack.distinctPayloads.size() >= 1, 10_000, "the first row");
                    server.dropAllConnections();
                    for (int i = 1; i < 20; i++) {
                        sender.table("t").longColumn("v", i).atNow();
                        sender.flush();
                    }
                    await(() -> errors.count("auth-rejected") >= 2, 15_000, "the 401 episode to be reported");
                    source.thenToken("T2", System.currentTimeMillis() + HOUR); // the IdP rotates
                    await(() -> ack.distinctPayloads.size() >= 20, 15_000, "every row to arrive");
                    Assert.assertTrue(sender.awaitAckedFsn(19, 10_000));
                }
                Assert.assertEquals(0, errors.terminalCount());
                Assert.assertEquals("no data-loss report", 0, errors.count(SenderError.Category.DATA_LOSS));
            }
            Assert.assertFalse("no quarantine", Files.exists(new java.io.File(sfDir, "rot/" + OrphanScanner.FAILED_SENTINEL_NAME).toPath()));
        });
    }

    @Test(timeout = 30_000)
    public void testSyncInitializationFailsFastOnAPermanentProviderFailure() throws Exception {
        // C12 / D8: a permanent credential-unavailable failure fails SYNC startup fast with the provider's error.
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenThrow(TokenUnavailableException.permanent("no managed identity is assigned"));
            try (TestWebSocketServer server = startServer(new AckHandler());
                 RefreshingTokenProvider provider = fastProvider(source).build()) {
                long start = System.nanoTime();
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .initialConnectMode(Sender.InitialConnectMode.SYNC)
                            .reconnectMaxDurationMillis(20_000)
                            .httpTokenProvider(provider)
                            .build()
                            .close();
                    Assert.fail();
                } catch (TokenUnavailableException e) {
                    Assert.assertFalse(e.isRetryable());
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no managed identity is assigned"));
                }
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                Assert.assertTrue("must fail fast, took " + elapsedMillis + " ms", elapsedMillis < 10_000);
                Assert.assertEquals("no endpoint may be contacted", 0, server.upgradeRequestCount());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSyncInitializationFailsFastOnAnUnclassifiedProviderException() throws Exception {
        // D8: an exception from an application-supplied provider that is not a token-unavailable error is
        // permanent, even when it says "retry".
        assertMemoryLeak(() -> {
            AtomicInteger pulls = new AtomicInteger();
            try (TestWebSocketServer server = startServer(new AckHandler())) {
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .initialConnectMode(Sender.InitialConnectMode.SYNC)
                            .reconnectMaxDurationMillis(20_000)
                            .httpTokenProvider(() -> {
                                pulls.incrementAndGet();
                                throw new IllegalStateException("temporarily unavailable, please retry");
                            })
                            .build()
                            .close();
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertTrue(e.getMessage().contains("temporarily unavailable"));
                }
                Assert.assertEquals(1, pulls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSyncInitializationRetriesARetryableProviderFailureWithinTheBudget() throws Exception {
        // C12 / D6: a retryable credential-unavailable failure is retried within reconnect_max_duration_millis.
        assertMemoryLeak(() -> {
            AtomicInteger pulls = new AtomicInteger();
            try (TestWebSocketServer server = startServer(new AckHandler());
                 Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                         .address("localhost:" + server.getPort())
                         .initialConnectMode(Sender.InitialConnectMode.SYNC)
                         .reconnectMaxDurationMillis(20_000)
                         .reconnectInitialBackoffMillis(10)
                         .reconnectMaxBackoffMillis(20)
                         .httpTokenProvider(() -> {
                             if (pulls.incrementAndGet() < 4) {
                                 throw TokenUnavailableException.retryable("IMDS returned HTTP 429", 0);
                             }
                             return "FINALLY";
                         })
                         .build()) {
                Assert.assertEquals("Bearer FINALLY", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                Assert.assertEquals(4, pulls.get());
                sender.table("t").longColumn("v", 1).atNow();
                sender.flush();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSyncInitializationRetryableProviderFailureExhaustsTheBudgetNamingTheClass() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger pulls = new AtomicInteger();
            try (TestWebSocketServer server = startServer(new AckHandler())) {
                try {
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + server.getPort())
                            .initialConnectMode(Sender.InitialConnectMode.SYNC)
                            .reconnectMaxDurationMillis(300)
                            .reconnectInitialBackoffMillis(10)
                            .reconnectMaxBackoffMillis(20)
                            .httpTokenProvider(() -> {
                                pulls.incrementAndGet();
                                throw TokenUnavailableException.retryable("IdP timed out");
                            })
                            .build()
                            .close();
                    Assert.fail();
                } catch (TokenUnavailableException e) {
                    Assert.fail("a retryable failure must consume the budget, not fail fast: " + e.getMessage());
                } catch (io.questdb.client.cutlass.line.LineSenderException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("credential-unavailable: IdP timed out"));
                }
                Assert.assertTrue("retried within the budget, pulls=" + pulls.get(), pulls.get() > 2);
                Assert.assertEquals(0, server.upgradeRequestCount());
            }
        });
    }

    private static void renderThrowable(Throwable t, List<String> into) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            into.add(c.toString());
            for (Throwable s : c.getSuppressed()) {
                renderThrowable(s, into);
            }
        }
    }

    private static List<String> drainHeaders(TestWebSocketServer server) throws InterruptedException {
        List<String> headers = new ArrayList<>();
        String h;
        while ((h = server.pollAuthorizationHeader(200, TimeUnit.MILLISECONDS)) != null) {
            headers.add(h);
        }
        return headers;
    }

    // Tokens shaped T<n>.<expires_at_ms>, as the specification's conformance harness suggests.
    private static long expiredAt(String header) {
        int dot = header.lastIndexOf('.');
        return dot < 0 ? Long.MIN_VALUE : Long.parseLong(header.substring(dot + 1));
    }

    private static RefreshingTokenProvider.Builder fastProvider(ScriptedSource source) {
        return RefreshingTokenProvider.builder(source)
                .coldWaitMillis(200)
                .backoffInitialMillis(10)
                .backoffMaxMillis(20)
                .forcedWaitMillis(5_000);
    }

    private static List<SenderConnectionEvent.Kind> kinds(List<SenderConnectionEvent> events) {
        List<SenderConnectionEvent.Kind> kinds = new ArrayList<>();
        for (SenderConnectionEvent e : events) {
            kinds.add(e.getKind());
        }
        return kinds;
    }

    private static String lastHeader(TestWebSocketServer server) throws InterruptedException {
        String last = null;
        String h;
        while ((h = server.pollAuthorizationHeader(100, TimeUnit.MILLISECONDS)) != null) {
            last = h;
        }
        return last;
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler)
            throws IOException, InterruptedException {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private void assertChallengeRetries(String challenge) throws Exception {
        assertMemoryLeak(() -> {
            ScriptedSource source = new ScriptedSource()
                    .thenToken("T1", System.currentTimeMillis() + HOUR)
                    .thenToken("T2", System.currentTimeMillis() + HOUR);
            try (TestWebSocketServer server = startServer(new AckHandler());
                 RefreshingTokenProvider provider = fastProvider(source).build()) {
                server.setAuthorizationValidator(h -> "Bearer T1".equals(h) ? 401 : 0);
                server.setRejectWwwAuthenticate(challenge);
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .httpTokenProvider(provider)
                        .build()) {
                    sender.table("t").longColumn("v", 1).atNow();
                    sender.flush();
                }
                Assert.assertEquals("challenge=" + challenge, Arrays.asList("Bearer T1", "Bearer T2"), drainHeaders(server));
                Assert.assertEquals(1, server.authRejectCount());
            }
        });
    }

    static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
    }

    /**
     * ACKs every frame, per connection: wire sequences restart at 0 on each connection.
     */
    static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong frames = new AtomicLong();
        private final java.util.Map<TestWebSocketServer.ClientHandler, AtomicLong> seqs =
                Collections.synchronizedMap(new java.util.IdentityHashMap<>());

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.incrementAndGet();
            long seq = seqs.computeIfAbsent(client, c -> new AtomicLong()).getAndIncrement();
            try {
                client.sendBinary(buildAck(seq));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Records every frame; ACKs them per connection when {@code ack} is set.
     */
    static final class CollectingHandler extends AckHandler {
        final Set<String> distinctPayloads = Collections.synchronizedSet(new HashSet<>());
        final List<String> payloads = new CopyOnWriteArrayList<>();
        private final boolean ack;

        CollectingHandler(boolean ack) {
            this.ack = ack;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            String p = Arrays.toString(data);
            distinctPayloads.add(p);
            payloads.add(p);
            if (ack) {
                super.onBinaryMessage(client, data);
            } else {
                frames.incrementAndGet();
            }
        }
    }

    /**
     * ACKs every frame; closes the first connection right after its first ACK, forcing a reconnect.
     */
    static final class DropAfterFirstAckHandler extends AckHandler {
        private volatile boolean dropped;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            super.onBinaryMessage(client, data);
            if (!dropped) {
                dropped = true;
                try {
                    Thread.sleep(50); // let the ACK flush before the socket goes
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                client.close();
            }
        }
    }

    static final class ErrorCollector implements io.questdb.client.SenderErrorHandler {
        final List<SenderError> errors = new CopyOnWriteArrayList<>();

        int count(String messageFragment) {
            int n = 0;
            for (SenderError e : errors) {
                if (e.getServerMessage() != null && e.getServerMessage().contains(messageFragment)) {
                    n++;
                }
            }
            return n;
        }

        int count(SenderError.Category category) {
            int n = 0;
            for (SenderError e : errors) {
                if (e.getCategory() == category) {
                    n++;
                }
            }
            return n;
        }

        @Override
        public void onError(SenderError error) {
            errors.add(error);
        }

        int terminalCount() {
            int n = 0;
            for (SenderError e : errors) {
                if (e.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                    n++;
                }
            }
            return n;
        }
    }

    // Real monotonic time, with a wall clock the test can push forward.
    static final class SkewedClock implements RefreshingTokenProvider.Clock {
        volatile long skewMillis;

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }

        @Override
        public long wallClockMillis() {
            return System.currentTimeMillis() + skewMillis;
        }
    }
}
