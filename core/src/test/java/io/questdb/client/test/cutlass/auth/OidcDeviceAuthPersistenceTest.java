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
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import io.questdb.client.cutlass.auth.PersistedToken;
import io.questdb.client.cutlass.auth.TokenStore;
import io.questdb.client.cutlass.auth.TokenStoreKey;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
            PrintStream originalErr = System.err;
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setErr(new PrintStream(captured, true, "UTF-8"));
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = baseBuilder(server).tokenStore(fake).build()) {
                // the save throws, but the sign-in still yields the valid in-memory token
                Assert.assertEquals("ACCESS-1", auth.signIn());
            } finally {
                System.setErr(originalErr);
            }
            String err = new String(captured.toByteArray(), StandardCharsets.UTF_8);
            Assert.assertTrue("a save failure must warn to System.err: " + err, err.contains("token store save failed"));
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
