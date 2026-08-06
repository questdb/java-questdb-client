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
import org.junit.Assert;
import org.junit.Rule;
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

    static {
        System.setProperty("questdb.client.oidc.open.browser", "false");
    }

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
                Files.createFile(dir.resolve(keyFor(server).hash() + ".lock"));
                try (OidcDeviceAuth auth = baseBuilder(server).tokenStore(new FileTokenStore(dir, 200, 600_000)).build()) {
                    long start = System.currentTimeMillis();
                    Assert.assertEquals("ACCESS-2", auth.getToken());
                    long elapsed = System.currentTimeMillis() - start;
                    Assert.assertTrue("getToken must degrade promptly, not stall, was " + elapsed, elapsed < 10_000);
                }
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
        // number of leading load() calls to fail before the first one is allowed to succeed; models a
        // transient store fault (an interrupted channel, a momentary IO error), as opposed to a store that
        // simply has nothing to return
        int failLoadTimes;
        boolean failSave;
        PersistedToken loadReturns;
        PersistedToken peerInstallsOnLock;
        PersistedToken stored;

        @Override
        public void clear(TokenStoreKey key) {
            clears.incrementAndGet();
            stored = null;
        }

        @Override
        public boolean inLock(TokenStoreKey key, CriticalSection action) {
            locks.incrementAndGet();
            if (peerInstallsOnLock != null) {
                // simulate a peer process refreshing and writing a fresh entry while we hold the lock
                stored = peerInstallsOnLock;
                peerInstallsOnLock = null;
            }
            return action.run();
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
