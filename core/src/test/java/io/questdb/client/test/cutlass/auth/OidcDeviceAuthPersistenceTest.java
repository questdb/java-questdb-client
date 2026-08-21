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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.cutlass.auth.FileTokenStore;
import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import io.questdb.client.cutlass.auth.PersistedToken;
import io.questdb.client.cutlass.auth.TokenStore;
import io.questdb.client.cutlass.auth.TokenStoreKey;
import io.questdb.client.test.tools.NoBrowserLaunch;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class OidcDeviceAuthPersistenceTest {
    private static final String DEVICE_PATH = "/device";
    private static final String TOKEN_PATH = "/token";

    // a restored sign-in here reaches the device-code prompt; see NoBrowserLaunch for why this is a rule
    @ClassRule
    public static final NoBrowserLaunch NO_BROWSER = new NoBrowserLaunch();

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testAdoptTrustsStoredIssuedTtlNotRemainingSpan() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-FRESH", "REFRESH-FRESH", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a token issued for 5m (stored ttl) loaded with only ~100s of life left. adopt() must set
                // tokenTtlMillis from the stored ISSUED lifetime (5m), NOT the remaining span (~100s): the
                // remaining-span form shrinks the effectiveSkewMillis basis as a token ages and collapses the
                // clock-skew margin near expiry (guarded by testAdoptedTokenNearExpiryStillRefreshesOnFlushPath).
                // A tampered ttl can only shrink the skew (never inflate it past CLOCK_SKEW_MILLIS) and the server
                // still enforces the real expiry, so trusting the stored value is no less safe.
                long now = System.currentTimeMillis();
                fake.loadReturns = new PersistedToken("ACCESS-1", null, "REFRESH-1", now + 100_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("the still-valid persisted token is served", "ACCESS-1", auth.signIn());
                    long ttl = readPrivateLong(auth, "tokenTtlMillis");
                    Assert.assertEquals("tokenTtlMillis must be the stored 5m issued lifetime, not the ~100s remaining span",
                            300_000L, ttl);
                }
                Assert.assertEquals("no device flow for a valid persisted token", 0, device.get());
                Assert.assertEquals("no refresh for a valid persisted token", 0, token.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAdoptedTokenNearExpiryStillRefreshesOnFlushPath() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-REFRESHED", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a token issued for 5m (stored ttl) but loaded with only ~20s of life left. The 30s clock-skew
                // margin exceeds the remaining life, so getToken() (the flush path) must silently refresh rather
                // than serve a token that would expire mid-request. Deriving the skew basis from the remaining
                // span (the pre-fix bug) collapses the margin to ~10s and serves the near-expired token instead.
                long now = System.currentTimeMillis();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-1", now + 20_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("a token inside the clock-skew margin must be refreshed, not served",
                            "ACCESS-REFRESHED", auth.getToken());
                }
                Assert.assertEquals("device flow must not run; a silent refresh suffices", 0, device.get());
                Assert.assertTrue("the token endpoint must be hit for the refresh", token.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCancelledStoreLockWaitDoesNotArmTheSharedRefreshBackOff() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // an expired access token with a live refresh token: getToken() must want a silent refresh
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-1",
                        System.currentTimeMillis() - 1_000, 300_000);
                fake.cancelWaitInsteadOfRunning = true;
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        auth.getToken();
                        Assert.fail("getToken must report the cancellation rather than a refresh failure");
                    } catch (OidcAuthException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("interrupted"));
                    }
                    // refreshFailedAtMillis is INSTANCE state shared by every producer holding this
                    // OidcDeviceAuth. Arming it here would fail all of them for
                    // MIN_REFRESH_RETRY_INTERVAL_MILLIS over a credential that is fine and an identity
                    // provider that was never contacted - the cancelled wait ran no refresh at all.
                    Assert.assertEquals("a cancelled lock wait must not arm the shared refresh back-off",
                            0L, readPrivateLong(auth, "refreshFailedAtMillis"));
                    Assert.assertEquals("the token endpoint must not have been called", 0, token.get());
                } finally {
                    // the store set the flag on this thread by design; do not leak it into later tests
                    Thread.interrupted();
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCancelledStoreLockWaitDoesNotStartTheDeviceFlow() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-1",
                        System.currentTimeMillis() - 1_000, 300_000);
                fake.cancelWaitInsteadOfRunning = true;
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        auth.signIn();
                        Assert.fail("signIn must decline once a cancellation abandoned the store lock wait");
                    } catch (OidcAuthException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("interrupted"));
                    }
                    // The device flow is the expensive wrong answer: it launches a browser and then polls
                    // to the device-code lifetime on Os.sleep, which ignores interrupts - so a caller that
                    // cancelled this thread cannot get it back, and shutdown does not complete. A plain
                    // false from inLock reads as "the refresh failed", which is exactly what sends signIn()
                    // here.
                    Assert.assertEquals("a cancelled lock wait must not start the interactive device flow",
                            0, device.get());
                    Assert.assertEquals("and must not reach the token endpoint either", 0, token.get());
                } finally {
                    Thread.interrupted();
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuildRejectsFileTokenStoreWithTooSmallStaleWindow() throws Exception {
        assertMemoryLeak(() -> {
            try (MockOidcServer server = new MockOidcServer((method, path, body) -> MockOidcServer.json(200, "{}"))) {
                Path dir = storeDir();
                // a lock-staleness window below LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE (4) x httpTimeoutMillis would let a
                // peer judge a live holder's lock stale and steal it mid-refresh, reopening the rotating-refresh-
                // token race the lock prevents; build() must reject the combination rather than ship the race
                try {
                    baseBuilder(server)
                            .httpTimeoutMillis(30_000)
                            .tokenStore(new FileTokenStore(dir, 3_000, 119_999))
                            .build();
                    Assert.fail("a lockStaleMillis below 4x httpTimeoutMillis must be rejected");
                } catch (OidcAuthException expected) {
                    Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("lockStaleMillis"));
                }
                // exactly 4x httpTimeoutMillis is the boundary and builds
                try (OidcDeviceAuth ignored = baseBuilder(server)
                        .httpTimeoutMillis(30_000)
                        .tokenStore(new FileTokenStore(dir, 3_000, 120_000))
                        .build()) {
                    // building at the boundary succeeds
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderRejectsHttpTimeoutAboveCap() throws Exception {
        assertMemoryLeak(() -> {
            // the HTTP timeout is capped (120s): a token-endpoint round-trip never needs longer, and bounding
            // it keeps a refresh held under the FileTokenStore cross-process lock safely shorter than that
            // store's staleness window, so a slow refresh's live lock is not stolen by a peer. Above the cap is
            // rejected; the boundary value builds.
            try {
                OidcDeviceAuth.builder().httpTimeoutMillis(120_001);
                Assert.fail("a httpTimeoutMillis above the cap must be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("httpTimeoutMillis"));
            }
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("questdb")
                    .deviceAuthorizationEndpoint("https://idp.example/device")
                    .tokenEndpoint("https://idp.example/token")
                    .httpTimeoutMillis(120_000)
                    .build()) {
                // building at the cap boundary succeeds
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClearCacheDeletesPersistedEntry() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                Path file = dir.resolve(keyFor(server).hash() + ".json");
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    auth.signIn();
                    Assert.assertTrue(Files.exists(file));

                    auth.clearCache();
                    Assert.assertFalse("clearCache must remove the persisted entry", Files.exists(file));

                    int before = device.get();
                    auth.signIn();
                    Assert.assertTrue("a cleared cache must re-run the device flow", device.get() > before);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClearCacheDoesNotReloadStaleEntry() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-NEW", null, "REFRESH-NEW", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // load() keeps returning a valid entry even after clear() (loadReturns is not cleared); this
                // proves clearCache() does not re-read the store - it relies on storeLoadAttempted, not on the
                // store having actually forgotten the entry - so a fresh device flow runs rather than re-adopting
                fake.loadReturns = new PersistedToken("ACCESS-OLD", null, "REFRESH-OLD",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-OLD", auth.signIn());
                    int loadsAfterFirst = fake.loads.get();

                    auth.clearCache();
                    Assert.assertEquals("clearCache must clear the persisted entry exactly once", 1, fake.clears.get());

                    // even though load() would still hand back ACCESS-OLD, clearCache must not let it be reloaded
                    Assert.assertEquals("ACCESS-NEW", auth.signIn());
                    Assert.assertEquals("clearCache must not trigger a re-read of the store",
                            loadsAfterFirst, fake.loads.get());
                    Assert.assertEquals("a cleared cache must re-run the device flow", 1, device.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDeviceGrantWithoutRefreshTokenClearsPreviousUsersRefreshToken() throws Exception {
        assertMemoryLeak(() -> {
            // Cross-account confusion. storeTokens() keeps the current refresh token whenever a response
            // omits one -- correct for a REFRESH response, which RFC 6749 6 lets omit it, but wrong for a
            // fresh device grant. A device grant is a NEW authorization and may be a DIFFERENT human: if it
            // returns no refresh token, the previous user's must not survive it, or the next silent refresh
            // signs back in as them with no interaction and no signal.
            //
            // A: persisted, expired access token plus a live refresh token.
            // B: signs in interactively after A's refresh hits a transient IdP failure, and B's grant carries
            // no refresh token of its own.
            AtomicInteger device = new AtomicInteger();
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                if (body.contains("grant_type=refresh_token")) {
                    // A's refresh token is live, not revoked -- the first attempt just lands on a 503. That is
                    // what makes this a confusion bug rather than a dead credential: the token still works, so
                    // any later use of it silently resumes A's session.
                    if (refreshCalls.incrementAndGet() == 1) {
                        return MockOidcServer.json(503, "{}");
                    }
                    return MockOidcServer.json(200, tokenJson("ACCESS-A2", null, "REFRESH-A", 3600));
                }
                // B's device grant: a served token and deliberately NO refresh token. expires_in=1 so B's
                // token goes stale inside the test without stubbing the clock.
                return MockOidcServer.json(200, tokenJson("ACCESS-B", null, null, 1));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.stored = new PersistedToken("ACCESS-A", null, "REFRESH-A", System.currentTimeMillis() - 1, 3600_000);

                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("A's refresh fails, so B signs in interactively",
                            "ACCESS-B", auth.signIn());
                    Assert.assertEquals("the device flow must have run for B", 1, device.get());

                    // B's token is issued for 1s and effectiveSkewMillis caps the skew at half that, so it
                    // reads as stale ~500ms in. Wait past that, then ask for a token the way the sender does.
                    Thread.sleep(1_000L);
                    try {
                        String served = auth.getToken();
                        Assert.fail("B's expired token must not be refreshed with A's retained refresh token; "
                                + "getToken() served [" + served + "] after " + refreshCalls.get() + " refresh calls");
                    } catch (OidcAuthException e) {
                        Assert.assertTrue("expected a prompt to sign in again, got: " + e.getMessage(),
                                e.getMessage().contains("could not be refreshed without an interactive sign-in"));
                    }
                    Assert.assertEquals("no refresh may be attempted once B's grant carried no refresh token",
                            1, refreshCalls.get());
                }

                // Persistence half: A's refresh token must not outlive B's sign-in on disk either, or the next
                // process start adopts it and resumes as A.
                Assert.assertNotNull("B's grant must have been persisted over A's entry", fake.stored);
                Assert.assertNull("A's refresh token must not survive in the store: " + fake.stored.getRefreshToken(),
                        fake.stored.getRefreshToken());

                // Restart over the same store.
                int refreshesBeforeRestart = refreshCalls.get();
                try (OidcDeviceAuth restarted = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        String served = restarted.getToken();
                        Assert.fail("a restart must not resume A's session; getToken() served [" + served + "]");
                    } catch (OidcAuthException e) {
                        Assert.assertTrue("expected a sign-in prompt after restart, got: " + e.getMessage(),
                                e.getMessage().contains("could not be refreshed without an interactive sign-in"));
                    }
                }
                Assert.assertEquals("a restart must not refresh with A's token either",
                        refreshesBeforeRestart, refreshCalls.get());
            }
        });
    }

    @Test
    public void testDefaultInLockRunsTheAction() {
        // TokenStore.inLock has a default that simply runs the action (no cross-process coordination). It is a
        // public extension point users implement, so a store that does NOT override inLock must still run its
        // critical section and return the action's result. Both in-tree stores override inLock, so this pins the
        // default directly; a regression to, say, "return false" without running the action would fail here.
        TokenStore store = new TokenStore() {
            @Override
            public void clear(TokenStoreKey key) {
            }

            @Override
            public PersistedToken load(TokenStoreKey key) {
                return null;
            }

            @Override
            public void save(TokenStoreKey key, PersistedToken token) {
            }
        };
        AtomicBoolean ran = new AtomicBoolean();
        boolean result = store.inLock(null, () -> {
            ran.set(true);
            return true;
        });
        Assert.assertTrue("the default inLock must run the action", ran.get());
        Assert.assertTrue("the default inLock must return the action's result", result);
    }

    @Test
    public void testEntryWithNoTokenOfEitherKindIsNotAdopted() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicBoolean sawPlantedRefreshToken = new AtomicBoolean();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                if (body != null && body.contains("REFRESH-PLANTED")) {
                    sawPlantedRefreshToken.set(true);
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // The cheapest credential swap there is: an entry carrying ONLY a refresh token. Every entry
                // this client writes carries at least one token kind, so this shape came from somewhere else -
                // an attacker who can WRITE the store directory, without ever reading our 0600 file. Adopted,
                // the next silent refresh would present THEIR refresh token and the client would resume as
                // them, with no prompt and nothing in any log recording the change of identity.
                fake.loadReturns = new PersistedToken(null, null, "REFRESH-PLANTED",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-FRESH", auth.signIn());
                }
                Assert.assertFalse("the planted refresh token must never reach the token endpoint",
                        sawPlantedRefreshToken.get());
                Assert.assertTrue("a rejected entry must fall back to the device flow, not to a silent refresh",
                        device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenBacksOffAfterAFailedRefreshInsteadOfFloodingTheIdp() throws Exception {
        assertMemoryLeak(() -> {
            // getToken() runs once per ILP flush and once per (re)connect, and a producer retrying its rows
            // calls it in a tight loop. Without a back-off a revoked refresh token cost a full
            // token-endpoint round trip on EVERY call: a sustained request flood at the provider - enough to
            // trip its rate limits and lengthen the outage being retried - and a producer blocked for each
            // round trip, up to the OS TCP-connect timeout against a black-holed endpoint.
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                tokenCalls.incrementAndGet();
                // the shape of a revoked refresh token
                return MockOidcServer.json(400, "{\"error\":\"invalid_grant\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-REVOKED",
                        System.currentTimeMillis() - 1, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    for (int i = 0; i < 25; i++) {
                        try {
                            auth.getToken();
                            Assert.fail("a revoked refresh token must not yield a usable token");
                        } catch (OidcAuthException expected) {
                            // every call still reports the failure - only the network attempt is rate-limited
                        }
                    }
                }
                Assert.assertEquals("25 getToken() calls must not mean 25 token-endpoint round trips",
                        1, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testGetTokenBackOffExpiresWhileAProducerKeepsCalling() throws Exception {
        assertMemoryLeak(() -> {
            // The companion of the throttle test above, and the half that cannot be checked by a tight
            // loop: the back-off must EXPIRE on its own while the caller keeps calling. Arming the latch
            // on a call the back-off itself skipped slides its window forward by one call every time, so
            // it never elapses for any caller returning faster than the retry interval - and getToken()
            // runs once per ILP flush, at a default auto-flush interval of one second. One transient
            // identity-provider failure then wedges the sender for the life of the process.
            //
            // Drives real wall clock because that is the only thing that distinguishes the two shapes:
            // both serve the same token, throttle to one round trip inside the window, and differ only in
            // whether a later call is ever allowed through.
            final long retryIntervalMillis = 5_000L; // OidcDeviceAuth.MIN_REFRESH_RETRY_INTERVAL_MILLIS
            AtomicBoolean healthy = new AtomicBoolean();
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                tokenCalls.incrementAndGet();
                if (!healthy.get()) {
                    // a transient outage, not a revoked grant: tryRefresh reports failure and leaves the
                    // cached refresh token intact, so the next attempt would succeed
                    return MockOidcServer.json(503, "{\"error\":\"temporarily_unavailable\"}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-RECOVERED", null, "REFRESH-NEW", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-OK",
                        System.currentTimeMillis() - 1, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        auth.getToken();
                        Assert.fail("the first call must attempt the refresh and surface its failure");
                    } catch (OidcAuthException expected) {
                    }
                    Assert.assertEquals("the first call must have reached the token endpoint",
                            1, tokenCalls.get());

                    // the provider recovers, and the caller keeps polling well inside the window, exactly
                    // as a flushing producer does
                    healthy.set(true);
                    String token = null;
                    final long deadlineNanos = System.nanoTime() + 4 * retryIntervalMillis * 1_000_000L;
                    while (token == null && System.nanoTime() - deadlineNanos < 0) {
                        Thread.sleep(50);
                        try {
                            token = auth.getToken();
                        } catch (OidcAuthException stillBackedOff) {
                        }
                    }
                    Assert.assertEquals("the back-off must expire on its own; the identity provider "
                                    + "recovered and getToken() never retried it",
                            "ACCESS-RECOVERED", token);
                    Assert.assertEquals("exactly one retry, once the window had elapsed",
                            2, tokenCalls.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSignInDeclinesOnAnInterruptCarryingThread() throws Exception {
        assertMemoryLeak(() -> {
            // The interrupt guard used to live only inside FileTokenStore.inLock, so signIn()'s behaviour
            // depended on whether a store was configured - and was wrong in OPPOSITE directions either way.
            //
            //   with a FileTokenStore: inLock declined the carried interrupt by returning false, signIn()
            //   read that as "the refresh failed" and started the DEVICE FLOW - a browser prompt and a poll
            //   loop Os.sleep cannot be interrupted out of, so the cancelled caller was parked for up to the
            //   device-code lifetime, having skipped a refresh it could have completed;
            //
            //   with no store: tryRefreshCoordinated() went straight to tryRefresh() and POSTed to the token
            //   endpoint on that same cancelled thread.
            //
            // Driven twice in one test on purpose: the defect was the ASYMMETRY, so the two configurations
            // agreeing is the property worth pinning. Both halves reach signIn() holding a usable refresh
            // token and an expired access token, so each has real network work to decline rather than a
            // cache hit to serve.
            for (boolean withStore : new boolean[]{true, false}) {
                AtomicInteger device = new AtomicInteger();
                AtomicInteger token = new AtomicInteger();
                MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
                try (MockOidcServer server = new MockOidcServer(handler)) {
                    OidcDeviceAuth.Builder builder = baseBuilder(server);
                    if (withStore) {
                        // a REAL FileTokenStore, not a double: its inLock is what declines a carried
                        // interrupt, and that decline is the half of the asymmetry that ended in a prompt
                        Path dir = storeDir();
                        new FileTokenStore(dir).save(keyFor(server),
                                new PersistedToken("ACCESS-STALE", null, "REFRESH-OK",
                                        System.currentTimeMillis() - 1, 300_000));
                        builder.tokenStore(new FileTokenStore(dir));
                    }
                    try (OidcDeviceAuth auth = builder.build()) {
                        if (!withStore) {
                            // nothing on disk to restore from, so earn a refresh token the ordinary way
                            Assert.assertEquals("ACCESS-1", auth.signIn());
                            OidcDeviceAuthTest.expireCachedToken(auth);
                        }
                        final int deviceBefore = device.get();
                        final int tokenBefore = token.get();

                        Thread.currentThread().interrupt();
                        try {
                            String served = auth.signIn();
                            Assert.fail("a cancelled caller must not be signed in [withStore=" + withStore
                                    + ", served=" + served + ", deviceFlows=" + (device.get() - deviceBefore)
                                    + ", tokenCalls=" + (token.get() - tokenBefore) + "]");
                        } catch (OidcAuthException e) {
                            Assert.assertTrue("withStore=" + withStore + ": " + e.getMessage(),
                                    e.getMessage().contains("interrupted"));
                        } finally {
                            Assert.assertTrue("the caller's cancellation signal must survive signIn() "
                                    + "[withStore=" + withStore + "]", Thread.interrupted());
                        }
                        Assert.assertEquals("no device flow may be started on a cancelled thread "
                                + "[withStore=" + withStore + "]", deviceBefore, device.get());
                        Assert.assertEquals("no token-endpoint round trip may be made on a cancelled thread "
                                + "[withStore=" + withStore + "]", tokenBefore, token.get());
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSignInClearsTheRefreshBackOff() throws Exception {
        assertMemoryLeak(() -> {
            // The back-off must never strand a caller: signIn() is the explicit action a user takes to
            // recover, so it re-attempts immediately and falls through to the device flow.
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                if (body != null && body.contains("REFRESH-REVOKED")) {
                    return MockOidcServer.json(400, "{\"error\":\"invalid_grant\"}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-NEW", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-REVOKED",
                        System.currentTimeMillis() - 1, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        auth.getToken();
                        Assert.fail("expected the revoked refresh to fail");
                    } catch (OidcAuthException expected) {
                        // now latched
                    }
                    Assert.assertEquals("ACCESS-FRESH", auth.signIn());
                    Assert.assertTrue("signIn() must not be held off by the back-off", device.get() >= 1);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenDeclinesTheRefreshOnAnInterruptCarryingThreadWithoutLatchingTheBackOff() throws Exception {
        assertMemoryLeak(() -> {
            // A cancelled caller must not drive a network round trip, must keep its cancellation signal, must
            // be told what actually happened, and must not suppress the next five seconds of legitimate
            // refreshes for every other thread sharing this instance.
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                tokenCalls.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-REFRESHED", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-1",
                        System.currentTimeMillis() - 1, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Thread.currentThread().interrupt();
                    final boolean flagSurvived;
                    final String message;
                    try {
                        auth.getToken();
                        Assert.fail("a cancelled caller must not get a token out of a network refresh");
                        return;
                    } catch (OidcAuthException e) {
                        message = e.getMessage();
                        flagSurvived = Thread.currentThread().isInterrupted();
                    } finally {
                        Thread.interrupted(); // do not leak the flag into the next test
                    }

                    Assert.assertEquals("no refresh may be attempted on a cancelled thread", 0, tokenCalls.get());
                    Assert.assertTrue("the message must name the interrupt, not blame the credential: " + message,
                            message.contains("interrupted"));
                    Assert.assertFalse("it must not send the user to re-authenticate: " + message,
                            message.contains("call signIn()"));
                    Assert.assertTrue("the caller's cancellation signal must survive getToken()", flagSurvived);

                    // and the decline must not have latched the back-off: a clean caller refreshes at once
                    Assert.assertEquals("ACCESS-REFRESHED", auth.getToken());
                    Assert.assertEquals(1, tokenCalls.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenDeclinesTheRefreshOnAnInterruptCarryingThreadWithNoTokenStore() throws Exception {
        assertMemoryLeak(() -> {
            // The only interrupt guard used to live inside FileTokenStore.inLock, so with NO store configured
            // a cancelled thread POSTed to the token endpoint regardless. The guard is in getToken() now, so
            // both shapes behave the same.
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                tokenCalls.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = baseBuilder(server).build()) {
                Assert.assertEquals("ACCESS-1", auth.signIn()); // one device grant, one token call
                Assert.assertEquals(1, tokenCalls.get());
                OidcDeviceAuthTest.expireCachedToken(auth);

                Thread.currentThread().interrupt();
                try {
                    auth.getToken();
                    Assert.fail("a cancelled caller must not drive a refresh even with no token store");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("interrupted"));
                } finally {
                    Thread.interrupted();
                }
                Assert.assertEquals("no refresh may be attempted on a cancelled thread", 1, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenAsFirstCallAfterRestore() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                new FileTokenStore(dir).save(keyFor(server),
                        new PersistedToken("ACCESS-1", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000));
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    // getToken() without a prior signIn(): a restored process can flush immediately
                    Assert.assertEquals("ACCESS-1", auth.getToken());
                }
                Assert.assertEquals(0, device.get());
                Assert.assertEquals(0, token.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenDegradesWhenStoreLockHeld() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                // a small acquire budget so the test does not wait the 3s default; a large staleness window so
                // the pre-created lock is treated as a live peer's and not stolen
                new FileTokenStore(dir, 200, 600_000).save(keyFor(server),
                        new PersistedToken("OLD-ACCESS", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000));
                // a peer holds the per-identity lock: getToken() must wait out only its short acquire budget,
                // then degrade to a lock-free refresh rather than stall the flush path or fail
                final int acquireBudgetMillis = 200;
                Path lock = dir.resolve(keyFor(server).hash() + ".lock");
                Files.createFile(lock);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(
                        new FileTokenStore(dir, acquireBudgetMillis, 600_000)).build()) {
                    long start = System.currentTimeMillis();
                    Assert.assertEquals("ACCESS-2", auth.getToken());
                    long elapsed = System.currentTimeMillis() - start;
                    // Bounded on BOTH sides, because one side alone cannot tell a degrade from an acquire.
                    // Below: the whole acquire budget must have been spent polling for a lock this call never
                    // gets - a run that skipped the wait (or took the lock) returns in single-digit millis.
                    // The peer's lock is empty, so only the 5s EMPTY_LOCK_STEAL_GRACE_MILLIS governs a steal,
                    // and a 200ms budget cannot reach it: the wait is deterministic, not racy.
                    Assert.assertTrue("getToken must wait out the acquire budget before degrading, was " + elapsed,
                            elapsed >= acquireBudgetMillis);
                    // Above: budget + one loopback refresh, with a wide margin. The old bound was 10s, which
                    // a regression to the 3s DEFAULT acquire budget would have sailed through.
                    Assert.assertTrue("getToken must degrade promptly, not stall, was " + elapsed, elapsed < 2_000);
                }
                // the definitive degrade-vs-acquire evidence: had getToken() acquired (or stolen) the lock, it
                // would have deleted the file on release
                Assert.assertTrue("the peer's lock must be left exactly where it was", Files.exists(lock));
                Assert.assertEquals("device flow must not run; getToken degrades to a lock-free refresh", 0, device.get());
                Assert.assertTrue("the refresh must hit the token endpoint", token.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNoStorePersistsNothing() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = baseBuilder(server).build()) {
                Assert.assertEquals("ACCESS-1", auth.signIn());
                Assert.assertEquals("ACCESS-1", auth.getToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNonRotatingRefreshDoesNotRewrite() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                if (body.contains("grant_type=refresh_token")) {
                    // a non-rotating provider returns no new refresh token
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", null, null, 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.stored = new PersistedToken("OLD", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-2", auth.signIn());
                }
                Assert.assertEquals("an unchanged refresh token must not rewrite the file", 0, fake.saves.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshUnderLockAdoptsPeerTokenAndSkipsNetwork() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // our own (expired) entry: adopted on load, leaving us in sync with what we last persisted
                fake.stored = new PersistedToken("OLD-ACCESS", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000);
                // a peer refreshes and writes a fresh, still-valid entry while we hold the cross-process lock
                fake.peerInstallsOnLock = new PersistedToken("PEER-ACCESS", null, "REFRESH-2", System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("must adopt the peer's fresh token from inside the lock", "PEER-ACCESS", auth.signIn());
                }
                Assert.assertEquals("a peer's still-valid token must be served without a token-endpoint call", 0, token.get());
                Assert.assertEquals("device flow must not run", 0, device.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshUnderLockKeepsLiveRefreshTokenWhenPeerEntryOmitsIt() throws Exception {
        // regression: a peer (or cross-language client, or a tampered file) persists a valid-but-expired served
        // token with NO refresh_token while we hold a live refresh token in memory. The coordinated re-read must
        // keep our refresh token, not null it - nulling it made tryRefresh() urlEncode(null) and throw an
        // uncaught NPE that aborted getToken()/signIn() instead of degrading. With the fix REFRESH-1 is kept and
        // the refresh succeeds.
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (path.startsWith(DEVICE_PATH)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                // the token endpoint honours the kept refresh token and returns a fresh access token
                return MockOidcServer.json(200, tokenJson("REFRESHED-ACCESS", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                long now = System.currentTimeMillis();
                // our own entry, adopted on load: an expired served token carrying REFRESH-1
                fake.stored = new PersistedToken("OLD-ACCESS", null, "REFRESH-1", now - 60_000, 300_000);
                // a peer overwrites the file with a valid-but-expired served token and NO refresh_token (the
                // frozen on-disk format permits omitting it) while we hold the cross-process lock
                fake.peerInstallsOnLock = new PersistedToken("PEER-ACCESS", null, null, now - 60_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("the live refresh token must be kept and used, not nulled into an NPE",
                            "REFRESHED-ACCESS", auth.getToken());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshUnderLockResavesKeptRefreshTokenWhenPeerEntryOmitsIt() throws Exception {
        // the other half of the NPE fix: when the coordinated re-read adopts a peer entry that omits the refresh
        // token, adopt() keeps the live refresh token AND records that the file carried none
        // (lastPersistedRefreshToken=null). The refresh that follows must therefore RE-SAVE the kept token, so a
        // restart still finds it on disk. If adopt() instead marked the kept token as already-persisted, the save
        // would be skipped and the refresh token would silently vanish from disk, forcing a needless re-prompt.
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (path.startsWith(DEVICE_PATH)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                // non-rotating refresh: the same REFRESH-1 comes back, so ONLY the null-vs-REFRESH-1 lastPersisted
                // bookkeeping (not a token change) decides whether persistIfRotated re-saves
                return MockOidcServer.json(200, tokenJson("REFRESHED-ACCESS", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                long now = System.currentTimeMillis();
                // our own entry, adopted on load: an expired served token carrying REFRESH-1
                fake.stored = new PersistedToken("OLD-ACCESS", null, "REFRESH-1", now - 60_000, 300_000);
                // a peer overwrites the file with an expired served token and NO refresh_token while we hold the
                // lock; the re-read adopts it, keeps REFRESH-1, and records that the file carried no refresh token
                fake.peerInstallsOnLock = new PersistedToken("PEER-ACCESS", null, null, now - 60_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("REFRESHED-ACCESS", auth.getToken());
                }
                Assert.assertTrue("the kept refresh token must be re-saved (the file carried none), not skipped as already-persisted",
                        fake.saves.get() >= 1);
                Assert.assertNotNull("the re-saved entry must exist", fake.stored);
                Assert.assertEquals("the re-saved entry must carry the kept refresh token", "REFRESH-1", fake.stored.getRefreshToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRestartRefreshesExpiredTokenSkippingDeviceFlow() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                // seed an already-expired access token plus a valid refresh token, as if persisted before a restart
                new FileTokenStore(dir).save(keyFor(server),
                        new PersistedToken("OLD-ACCESS", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000));
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    Assert.assertEquals("ACCESS-2", auth.signIn());
                }
                Assert.assertEquals("device flow must not run; a silent refresh suffices", 0, device.get());
                Assert.assertTrue("the token endpoint must be hit for the refresh", token.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRestartServesPersistedIdTokenWithGroupsInToken() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                token.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-1", "ID-1", "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                try (OidcDeviceAuth first = baseBuilder(server).groupsInToken(true).tokenStore(new FileTokenStore(dir)).build()) {
                    // with groups encoded in the token, signIn() serves the id token, and that is what persists
                    Assert.assertEquals("ID-1", first.signIn());
                }
                Assert.assertEquals(1, device.get());
                // the persisted entry must record the id-token identity, so an access-token-mode client rejects it
                String json = new String(Files.readAllBytes(dir.resolve(keyForGroups(server).hash() + ".json")), StandardCharsets.UTF_8);
                Assert.assertTrue("file must record groups_in_token=true: " + json, json.contains("\"groups_in_token\":true"));
                device.set(0);
                token.set(0);

                // a restart over the same store serves the persisted id token with no network
                try (OidcDeviceAuth restarted = baseBuilder(server).groupsInToken(true).tokenStore(new FileTokenStore(dir)).build()) {
                    Assert.assertEquals("ID-1", restarted.signIn());
                }
                Assert.assertEquals("device flow must not run on restart", 0, device.get());
                Assert.assertEquals("a valid persisted id token needs no token-endpoint call", 0, token.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRestartServesPersistedTokenWithoutNetwork() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                try (OidcDeviceAuth first = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    Assert.assertEquals("ACCESS-1", first.signIn());
                }
                Assert.assertEquals(1, device.get());
                device.set(0);
                token.set(0);

                // a new instance over the same store mimics a restart: it serves the persisted (still valid)
                // token with no calls to either endpoint
                try (OidcDeviceAuth restarted = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    Assert.assertEquals("ACCESS-1", restarted.signIn());
                }
                Assert.assertEquals("device flow must not run on restart", 0, device.get());
                Assert.assertEquals("a valid persisted token needs no token-endpoint call", 0, token.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRotatingRefreshRewritesStore() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.stored = new PersistedToken("OLD", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-2", auth.signIn());
                }
                Assert.assertEquals(0, device.get());
                Assert.assertEquals("the coordinated refresh must run through the store's cross-process lock", 1, fake.locks.get());
                Assert.assertEquals("a rotated refresh token must be persisted", 1, fake.saves.get());
                Assert.assertEquals("REFRESH-2", fake.stored.getRefreshToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSaveFailureIsNonFatal() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            FakeTokenStore fake = new FakeTokenStore();
            fake.failSave = true;
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                // the store's save throws, but the failure is swallowed (warned best-effort, then ignored) and
                // the sign-in still yields the valid in-memory token
                Assert.assertEquals("ACCESS-1", auth.signIn());
            }
            // the save was actually attempted, so the throwing path was exercised rather than skipped
            Assert.assertTrue("the token store save must have been attempted", fake.saves.get() >= 1);
        });
    }

    @Test(timeout = 30_000)
    public void testSaveFailureThenRefreshDoesNotReplayRevokedToken() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicBoolean refresh1Consumed = new AtomicBoolean();
            // a rotating identity provider: REFRESH-1 mints REFRESH-2 once (and is then revoked, so replaying
            // it is rejected); REFRESH-2 mints REFRESH-3. The first refreshed token is short-lived, forcing a
            // second refresh while the rotated REFRESH-2 is still only in memory (every save fails).
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                if (body.contains("refresh_token=REFRESH-2")) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-3", null, "REFRESH-3", 3600));
                }
                if (body.contains("refresh_token=REFRESH-1")) {
                    if (refresh1Consumed.compareAndSet(false, true)) {
                        return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 1));
                    }
                    // a rotated-away refresh token is revoked: replaying it must be rejected
                    return MockOidcServer.json(400, "{\"error\":\"invalid_grant\"}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.failSave = true; // every persist fails, so the rotated REFRESH-2 never reaches disk
                fake.stored = new PersistedToken("OLD-ACCESS", null, "REFRESH-1", System.currentTimeMillis() - 60_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    // first refresh rotates REFRESH-1 -> REFRESH-2 (save fails, so disk still says REFRESH-1)
                    Assert.assertEquals("ACCESS-2", auth.signIn());
                    // let the short-lived access token expire so the next call refreshes again
                    Thread.sleep(1_200);
                    // the second refresh must use the in-memory REFRESH-2, not re-read the stale (now revoked)
                    // REFRESH-1 from disk - otherwise the replay is rejected and we are forced to re-prompt
                    Assert.assertEquals("ACCESS-3", auth.signIn());
                }
                Assert.assertEquals("a swallowed save must not force the device flow on the next refresh", 0, device.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testStoreThrowingBeforeTheActionDegradesToOneUncoordinatedRefresh() throws Exception {
        assertMemoryLeak(() -> {
            // TokenStore is a user-implemented SPI and persistence is documented best-effort, but inLock was
            // called bare: a store that threw took the whole sign-in down with it and refreshed nothing, even
            // though the client held a perfectly good refresh token. The degrade is a single uncoordinated
            // refresh - exactly one, because the lock exists to stop a rotating refresh token being POSTed
            // twice, and a reuse-detecting provider answers a replay by revoking the whole family.
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-REFRESHED", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.stored = new PersistedToken("ACCESS-STALE", null, "REFRESH-1", System.currentTimeMillis() - 1, 300_000);
                fake.throwBeforeAction = new RuntimeException("LOCK-DOWN");
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("a throwing store must not fail a sign-in it cannot help with",
                            "ACCESS-REFRESHED", auth.getToken());
                }
                Assert.assertEquals("the degrade must still refresh", 1, token.get());
                Assert.assertEquals("the interactive flow must not be needed", 0, device.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testStoreThrowingAfterTheActionKeepsTheCompletedRefresh() throws Exception {
        assertMemoryLeak(() -> {
            // The mirror case, and the one where a blind retry does real damage. The store threw on the way
            // OUT - releasing its lock, closing a handle - so the refresh already happened and the token is
            // live. Re-running it would be the duplicate POST of a rotating refresh token the lock exists to
            // prevent, and propagating would tell the caller a completed sign-in failed.
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-1", "REFRESH-1", "ACCESS-REFRESHED", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                fake.stored = new PersistedToken("ACCESS-STALE", null, "REFRESH-1", System.currentTimeMillis() - 1, 300_000);
                fake.throwAfterAction = new RuntimeException("RELEASE-FAILED");
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("a completed refresh must be reported, not undone by a bookkeeping throw",
                            "ACCESS-REFRESHED", auth.getToken());
                }
                Assert.assertEquals("the refresh must not be replayed after it already completed",
                        1, token.get());
                Assert.assertEquals("the interactive flow must not be needed", 0, device.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testStoreLoadedAtMostOncePerInstance() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a valid persisted token, so every getToken()/signIn() is a cache hit
                fake.loadReturns = new PersistedToken("ACCESS-1", null, "REFRESH-1",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    auth.getToken();
                    auth.signIn();
                    auth.getToken();
                    Assert.assertEquals("the store must be read at most once per instance, not on every call",
                            1, fake.loads.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedBareJsonNullServedTokenIsRefusedNotServedAsBearerNull() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-FRESH", "REFRESH-FRESH", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                // Start from a file a conforming writer produced, so the fingerprint and the file name are
                // exactly right, then replace the served token with a BARE JSON null - the one encoding
                // design/oidc-token-persistence.md forbids, and the natural output of
                // json.dumps({"access_token": None}) in a peer client sharing this store.
                new FileTokenStore(dir).save(keyFor(server),
                        new PersistedToken("ACCESS-PLANTED", null, "REFRESH-1",
                                System.currentTimeMillis() + 300_000, 300_000));
                Path file = dir.resolve(keyFor(server).hash() + ".json");
                String conforming = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
                Assert.assertTrue("the writer must emit a present token as a QUOTED string, or this test is "
                                + "not planting what it thinks: " + conforming,
                        conforming.contains("\"access_token\":\"ACCESS-PLANTED\""));
                Files.write(file, conforming
                        .replace("\"access_token\":\"ACCESS-PLANTED\"", "\"access_token\":null")
                        .getBytes(StandardCharsets.UTF_8));

                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    // JsonLexer reports a bare null and a quoted "null" identically, so the entry reaches
                    // adopt() as the four characters "null" - non-blank, printable ASCII, and with a
                    // fingerprint that matches, so nothing before adopt() turns it away. Served, it becomes
                    // "Bearer null", which the server answers with 401; and because the persisted expiry is
                    // still valid, getToken() would go on serving it rather than refreshing, so the producer
                    // 401s with nothing naming the cause until the expiry lapses.
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("a bare JSON null must never be served as the credential",
                            "null", result);
                    Assert.assertNotEquals("null", auth.getToken());
                    Assert.assertEquals("Bearer ACCESS-FRESH", auth.getAuthorizationHeaderValue());
                }
                Assert.assertTrue("a bare JSON null must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedBlankServedTokenRejectedOnLoad() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a tampered persisted entry with a BLANK (whitespace-only) served token is NOT isEmpty() and
                // passes hasOnlyTokenChars vacuously (space is 0x20), yet is served as a blank "Bearer " header
                // the server only answers with 401 - so adopt() must reject it (via Chars.isBlank, matching the
                // sender's own HttpTokenProvider.validateToken) and fall back to the device flow, not wedge on it
                fake.loadReturns = new PersistedToken("   ", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("   ", result);
                }
                Assert.assertTrue("a rejected blank persisted token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedEmptyServedTokenRejectedOnLoad() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a tampered persisted entry with an EMPTY served token passes hasOnlyTokenChars vacuously but
                // would be served as a blank "Bearer " header; adopt() must reject it (not serve "") and fall
                // back to the device flow, exactly like a control-char token
                fake.loadReturns = new PersistedToken("", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("", result);
                }
                Assert.assertTrue("a rejected empty persisted token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedFarFutureExpiryIsBoundedNotTrustedForever() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-FRESH", "REFRESH-FRESH", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a tampered entry claims the access token never expires. adopt() must clamp the trust window
                // to MAX_EXPIRES_IN_SECONDS rather than copy the far-future expiry verbatim, and the clamp
                // arithmetic (now + maxLife, Math.min over the persisted value) must not overflow on
                // Long.MAX_VALUE. Within the clamped hour the token is still valid, so it is served with no
                // network.
                fake.loadReturns = new PersistedToken("ACCESS-1", null, "REFRESH-1", Long.MAX_VALUE, Long.MAX_VALUE);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("a still-valid persisted token is served within the clamped window", "ACCESS-1", auth.signIn());
                    // assert the clamp actually bounds the trust window, not merely that it avoids overflow:
                    // a verbatim copy of the Long.MAX_VALUE expiry would still pass the assertion above but
                    // fail these. MAX_EXPIRES_IN_SECONDS is 3600, so the window is at most one hour from now.
                    long maxLifeMillis = 3_600_000L;
                    Assert.assertTrue("a tampered far-future expiry must be clamped to <= now + 1h",
                            readPrivateLong(auth, "expiresAtMillis") <= System.currentTimeMillis() + maxLifeMillis);
                    Assert.assertTrue("a tampered ttl must be clamped to <= 1h",
                            readPrivateLong(auth, "tokenTtlMillis") <= maxLifeMillis);
                }
                Assert.assertEquals("no device flow for a valid persisted token", 0, device.get());
                Assert.assertEquals("no token-endpoint call for a valid persisted token", 0, token.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedFarPastExpiryIsNotServedAsValid() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-FRESH", "REFRESH-FRESH", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a tampered entry claims an absurd, far-PAST expiry near Long.MIN_VALUE. adopt() clamps the
                // expiry to [0, now + maxLife]; flooring at 0 is what keeps the validity check
                // (now < expiresAtMillis - skew) underflow-safe - without the floor a near-Long.MIN_VALUE
                // expiry wraps that subtraction to a huge positive and would serve the garbage-expiry token as
                // valid forever. It must instead read as expired and fall back to a silent refresh.
                fake.loadReturns = new PersistedToken("ACCESS-1", null, "REFRESH-1", Long.MIN_VALUE, Long.MIN_VALUE);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("a far-past expiry must not be served; the refresh token supplies a fresh one", "ACCESS-2", result);
                    Assert.assertNotEquals("a garbage-expiry token must never be served as valid", "ACCESS-1", result);
                }
                Assert.assertEquals("a valid refresh token needs no device flow", 0, device.get());
                Assert.assertTrue("the expired persisted token must trigger a token-endpoint refresh", token.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedFileWithCrlfTokenFallsBackToDeviceFlow() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = countingHandler(device, token, "ACCESS-FRESH", "REFRESH-FRESH", "ACCESS-2", "REFRESH-2");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                // a genuine on-disk file (valid fingerprint) whose served token carries CR/LF: the JSON writer
                // escapes it and the lexer decodes it back to real control bytes on load, so adopt() must
                // reject it and fall back rather than route a header-injecting credential onto the wire
                new FileTokenStore(dir).save(keyFor(server),
                        new PersistedToken("AC\r\nCESS", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000));
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir)).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("AC\r\nCESS", result);
                }
                Assert.assertTrue("a rejected on-disk token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedIdTokenRejectedOnLoadWithGroupsInToken() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                token.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", "ID-FRESH", "REFRESH-FRESH", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                Path dir = storeDir();
                // groups-in-token mode serves the ID token, so adopt() must validate the ID token, not the access
                // token. A genuine on-disk file (valid groups fingerprint) with a CLEAN access token but a CR/LF
                // id token must be rejected and fall back to the device flow, never routing the tampered id token
                // onto the wire. A bug that validated the access token would accept this entry and serve "I\r\nD".
                new FileTokenStore(dir).save(keyForGroups(server),
                        new PersistedToken("ACCESS-CLEAN", "I\r\nD", "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000));
                try (OidcDeviceAuth auth = baseBuilder(server).groupsInToken(true).tokenStore(new FileTokenStore(dir)).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ID-FRESH", result);
                    Assert.assertNotEquals("I\r\nD", result);
                }
                Assert.assertTrue("a rejected on-disk id token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedServedTokenRejectedOnLoad() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // a tampered persisted access token carrying CR/LF must never be served
                fake.loadReturns = new PersistedToken("AC\r\nCESS", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("AC\r\nCESS", result);
                }
                Assert.assertTrue("a rejected persisted token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTamperedServedTokenWithNonAsciiRejectedOnLoad() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // hasOnlyTokenChars rejects a non-ASCII char (> 0x7e), not just a control char: a persisted served
                // token carrying one (here U+00E9) is the byte the ASCII Authorization-header writer would
                // truncate, so adopt() must reject the entry and fall back rather than serve a corrupt credential
                fake.loadReturns = new PersistedToken("ACC\u00e9SS", null, "REFRESH-1", System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    String result = auth.signIn();
                    Assert.assertEquals("ACCESS-FRESH", result);
                    Assert.assertNotEquals("ACC\u00e9SS", result);
                }
                Assert.assertTrue("a rejected non-ASCII persisted token must fall back to the device flow", device.get() >= 1);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPersistedEntryWithoutServedTokenStillRefreshes() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            AtomicInteger token = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                token.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-REFRESHED", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // An entry with a good refresh token but no SERVED kind is reachable: under
                // groupsInToken=false a grant that returns only an id_token has storeTokens null the access
                // token, and persistIfRotated writes the entry anyway - and a cross-language peer can produce
                // the same shape. adopt() used to discard such an entry whole, throwing away the refresh
                // token, which is the one thing persistence exists to preserve. The restart must therefore
                // spend one silent refresh, not send a human back through the device flow.
                fake.loadReturns = new PersistedToken(null, "ID-1", "REFRESH-1",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-REFRESHED", auth.signIn());
                }
                Assert.assertEquals("the persisted refresh token must be spent on a silent refresh",
                        1, token.get());
                Assert.assertEquals("a usable persisted refresh token must not force the device flow",
                        0, device.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTransientStoreLoadFailureIsRetriedNotLatched() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // The first read fails transiently - the shape a carried interrupt flag produces, since
                // FileChannel is an InterruptibleChannel and throws ClosedByInterruptException on a thread
                // that merely carries the flag. Latching "already attempted" on that failure disables
                // persistence for the whole life of the instance, so a process holding a perfectly good
                // refresh token on disk re-runs the interactive device flow instead - a hard failure for the
                // headless getToken() consumer persistence exists for, not a degraded one. Only a read that
                // COMPLETES (even yielding nothing) is a definitive answer worth latching.
                fake.failLoadTimes = 1;
                fake.loadReturns = new PersistedToken("ACCESS-PERSISTED", null, "REFRESH-1",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    try {
                        auth.getToken();
                        Assert.fail("the first call must report no usable token after the read failed");
                    } catch (OidcAuthException expected) {
                        // the read threw, so nothing was adopted and there is no token to serve yet
                    }
                    Assert.assertEquals("the failed read must not be retried within one call", 1, fake.loads.get());

                    // the store recovers: the very next call must re-read it and serve the persisted token
                    Assert.assertEquals("ACCESS-PERSISTED", auth.getToken());
                    Assert.assertEquals("a failed read must leave the store re-readable", 2, fake.loads.get());
                    Assert.assertEquals("a recovered store must not force the interactive device flow",
                            0, device.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRepeatedStoreLoadFailureIsThrottledNotRetriedOnEveryCall() throws Exception {
        assertMemoryLeak(() -> {
            // A store that never becomes readable - a chmod or uid mismatch in a container, EIO/ESTALE on an
            // NFS home - must not cost a blocking read on every call. maybeLoadFromStore() runs on the
            // getToken() path AHEAD of the cache check, and getToken() runs once per ILP flush, so without a
            // back-off the producer thread paid a file open, two stack trace fills and a WARN line per flush,
            // forever, while holding this instance's lock.
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // never recovers, unlike the single transient fault above
                fake.failLoadTimes = Integer.MAX_VALUE;
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    for (int i = 0; i < 25; i++) {
                        try {
                            auth.getToken();
                            Assert.fail("an unreadable store leaves no token to serve");
                        } catch (OidcAuthException expected) {
                            // every call still reports the failure - only the store read is rate-limited
                        }
                    }
                    // two reads, not 25: the first failure, plus the free retry it arms so a one-shot fault
                    // still recovers at once. The second failure arms the real back-off, which the remaining
                    // 23 calls run inside of.
                    Assert.assertEquals("25 getToken() calls must not mean 25 store reads", 2, fake.loads.get());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testARotatedRefreshWithNoServedKindIsNotPersistedAsAnUnadoptableEntry() throws Exception {
        assertMemoryLeak(() -> {
            // adopt() rejects a refresh token carried with NEITHER token kind, treating it as positive
            // evidence of a foreign writer - an attacker who can write the store dropping in their own
            // refresh token. That reasoning only holds while this client cannot produce the shape.
            //
            // It can. Under groupsInToken the served kind is the id token, so a stored entry carrying only
            // an access token takes adopt()'s served-kind-absent branch, which nulls BOTH kinds and keeps
            // the refresh token. A refresh that then rotates the refresh token but still returns no id
            // token reaches adoptRotatedRefreshToken() -> persistIfRotated() with both null, and writing
            // that snapshot leaves a file this client refuses for the life of the entry: every restart
            // re-runs the device flow over a refresh token sitting on disk, which for a headless
            // getToken() consumer is a hard failure.
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                // rotates the refresh token, still no id_token: the grant the branch above exists for
                return MockOidcServer.json(200, tokenJson("ACCESS-NEW", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                long now = System.currentTimeMillis();
                PersistedToken seeded = new PersistedToken("ACCESS-OLD", null, "REFRESH-1",
                        now + 300_000, 300_000);
                fake.stored = seeded;
                fake.loadReturns = seeded;
                try (OidcDeviceAuth auth = baseBuilder(server).groupsInToken(true).tokenStore(fake).build()) {
                    try {
                        // no id token anywhere: the seeded entry has none and the refresh does not produce
                        // one, so this necessarily fails - the point is what it leaves on disk
                        auth.getToken();
                        Assert.fail("groupsInToken with no id token must not yield a served token");
                    } catch (OidcAuthException expected) {
                        // expected: selectToken() reports the missing served kind
                    }
                    Assert.assertEquals("the refresh must have run, or this test proves nothing about what "
                            + "adoptRotatedRefreshToken() persists", 0, device.get());

                    PersistedToken after = fake.stored;
                    Assert.assertNotNull("the pre-existing entry must not be replaced by nothing", after);
                    Assert.assertFalse("the client must never persist the one shape adopt() rejects as "
                                    + "foreign: a refresh token with neither token kind",
                            after.getAccessToken() == null && after.getIdToken() == null);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testARecoveredStoreReadDoesNotRevertACompletedSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // maybeLoadFromStore() deliberately leaves its latch UNSET when a read THROWS, so a transient
            // fault is retried rather than disabling persistence for the life of the instance. But it runs
            // at the top of getToken(), AHEAD of the cache check, and adopt() assigns the served kind, the
            // expiry and the ttl unconditionally - with no comparison against what is already in memory.
            //
            // So a store that is unavailable across signIn() and readable afterwards used to undo it: an
            // unmounted home or a container started before its volume attaches fails the read AND the save
            // (one root cause, both through ensureDirectory), the human authenticates, and then the next
            // getToken() - one per ILP flush - re-reads and installs the PREVIOUS entry over the grant just
            // obtained. The failed save is what makes it stick: nothing rewrote the entry to match memory.
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-FRESH", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                long now = System.currentTimeMillis();
                // a PREVIOUS login, still unexpired, so adopt() would take it and getToken() would serve it
                fake.loadReturns = new PersistedToken("ACCESS-STALE", null, "REFRESH-STALE",
                        now + 300_000, 300_000);
                fake.failLoadTimes = 1;  // the read inside signIn()
                fake.failSave = true;    // ...and the save that follows it, same root cause
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    Assert.assertEquals("ACCESS-FRESH", auth.signIn());
                    Assert.assertEquals("the device flow must have run", 1, device.get());
                    Assert.assertTrue("the save must have been attempted and failed, or this test is not "
                            + "reproducing the stuck-stale-entry case", fake.saves.get() > 0);

                    // the store is readable again from here on (failLoadTimes is spent)
                    Assert.assertEquals("a recovered store read must not install a previous login over the "
                            + "grant signIn() just obtained", "ACCESS-FRESH", auth.getToken());
                    Assert.assertEquals("ACCESS-FRESH", auth.getToken());
                    Assert.assertEquals("once this instance holds its own tokens the store is no longer "
                            + "authoritative for it and must not be re-read", 1, fake.loads.get());
                }
            }
        });
    }


    @Test(timeout = 30_000)
    public void testSignInClearsTheStoreLoadBackOff() throws Exception {
        assertMemoryLeak(() -> {
            // The back-off must never strand a caller: signIn() is the explicit action a user takes to
            // recover, and sending a human through the device flow over a refresh token that is sitting on
            // disk - readable again by then - is exactly what persistence exists to avoid.
            AtomicInteger device = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    device.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthJson());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-FRESH", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                FakeTokenStore fake = new FakeTokenStore();
                // the two getToken() reads fail, arming the back-off; the store is readable again by the time
                // signIn() is called
                fake.failLoadTimes = 2;
                fake.loadReturns = new PersistedToken("ACCESS-PERSISTED", null, "REFRESH-1",
                        System.currentTimeMillis() + 300_000, 300_000);
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                    for (int i = 0; i < 2; i++) {
                        try {
                            auth.getToken();
                            Assert.fail("the store read failed, so there is no token to serve yet");
                        } catch (OidcAuthException expected) {
                            // the second failure arms the back-off
                        }
                    }
                    Assert.assertEquals(2, fake.loads.get());

                    Assert.assertEquals("ACCESS-PERSISTED", auth.signIn());
                    Assert.assertEquals("signIn() must re-read a store the back-off is holding off",
                            3, fake.loads.get());
                    Assert.assertEquals("a readable store must not force the interactive device flow",
                            0, device.get());
                }
            }
        });
    }

    private static OidcDeviceAuth.Builder baseBuilder(MockOidcServer server) {
        return OidcDeviceAuth.builder()
                .clientId("questdb")
                .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                .scope("openid")
                .allowInsecureTransport(true)
                .prompt(challenge -> {
                });
    }

    private static MockOidcServer.Handler countingHandler(
            AtomicInteger device, AtomicInteger token,
            String access, String refresh, String refreshedAccess, String refreshedRefresh
    ) {
        return (method, path, body) -> {
            if (DEVICE_PATH.equals(path)) {
                device.incrementAndGet();
                return MockOidcServer.json(200, deviceAuthJson());
            }
            token.incrementAndGet();
            if (body.contains("grant_type=refresh_token")) {
                return MockOidcServer.json(200, tokenJson(refreshedAccess, null, refreshedRefresh, 3600));
            }
            return MockOidcServer.json(200, tokenJson(access, null, refresh, 3600));
        };
    }

    private static String deviceAuthJson() {
        return "{\"device_code\":\"DEV-CODE\",\"user_code\":\"WDJB-MJHT\","
                + "\"verification_uri\":\"https://verify.example/device\",\"expires_in\":300,\"interval\":1}";
    }

    private static TokenStoreKey keyFor(MockOidcServer server) {
        return new TokenStoreKey(
                "questdb",
                "http://127.0.0.1:" + server.port() + TOKEN_PATH,
                "http://127.0.0.1:" + server.port() + DEVICE_PATH,
                "openid",
                null,
                false);
    }

    private static TokenStoreKey keyForGroups(MockOidcServer server) {
        return new TokenStoreKey(
                "questdb",
                "http://127.0.0.1:" + server.port() + TOKEN_PATH,
                "http://127.0.0.1:" + server.port() + DEVICE_PATH,
                "openid",
                null,
                true);
    }

    private static long readPrivateLong(Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static String tokenJson(String access, String id, String refresh, int expiresIn) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"token_type\":\"Bearer\",\"expires_in\":").append(expiresIn);
        if (access != null) {
            sb.append(",\"access_token\":\"").append(access).append('"');
        }
        if (id != null) {
            sb.append(",\"id_token\":\"").append(id).append('"');
        }
        if (refresh != null) {
            sb.append(",\"refresh_token\":\"").append(refresh).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private Path storeDir() {
        return temp.getRoot().toPath().resolve("oidc-tokens");
    }

    private static final class FakeTokenStore implements TokenStore {
        final AtomicInteger clears = new AtomicInteger();
        final AtomicInteger loads = new AtomicInteger();
        final AtomicInteger locks = new AtomicInteger();
        final AtomicInteger saves = new AtomicInteger();
        // models a CONFORMANT coordinating store whose lock wait a cancellation abandoned: per TokenStore's
        // contract it returns false without running the action AND leaves the interrupt flag set, which is
        // what lets OidcDeviceAuth tell that apart from a refresh that ran and failed
        boolean cancelWaitInsteadOfRunning;
        // number of leading load() calls to fail before the first one is allowed to succeed; models a
        // transient store fault (an interrupted channel, a momentary IO error), as opposed to a store that
        // simply has nothing to return
        int failLoadTimes;
        boolean failSave;
        PersistedToken loadReturns;
        PersistedToken peerInstallsOnLock;
        PersistedToken stored;
        RuntimeException throwAfterAction;
        RuntimeException throwBeforeAction;

        @Override
        public void clear(TokenStoreKey key) {
            clears.incrementAndGet();
            stored = null;
        }

        @Override
        public boolean inLock(TokenStoreKey key, CriticalSection action) {
            locks.incrementAndGet();
            if (throwBeforeAction != null) {
                throw throwBeforeAction;
            }
            if (cancelWaitInsteadOfRunning) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (peerInstallsOnLock != null) {
                // simulate a peer process refreshing and writing a fresh entry while we hold the lock
                stored = peerInstallsOnLock;
                peerInstallsOnLock = null;
            }
            boolean result = action.run();
            if (throwAfterAction != null) {
                // a bookkeeping failure on the way out - releasing the lock, closing a handle - AFTER the
                // critical section already completed
                throw throwAfterAction;
            }
            return result;
        }

        @Override
        public PersistedToken load(TokenStoreKey key) {
            loads.incrementAndGet();
            if (failLoadTimes > 0) {
                failLoadTimes--;
                throw new RuntimeException("token store read failed");
            }
            return loadReturns != null ? loadReturns : stored;
        }

        @Override
        public void save(TokenStoreKey key, PersistedToken token) {
            saves.incrementAndGet();
            if (failSave) {
                throw new RuntimeException("disk full");
            }
            stored = token;
        }
    }
}
