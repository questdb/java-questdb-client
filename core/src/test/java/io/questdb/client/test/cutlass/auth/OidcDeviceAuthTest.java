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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.DeviceAuthorizationChallenge;
import io.questdb.client.cutlass.auth.DeviceCodePrompt;
import io.questdb.client.cutlass.auth.OidcAuthException;
import io.questdb.client.cutlass.auth.OidcDeviceAuth;
import io.questdb.client.cutlass.json.JsonException;
import io.questdb.client.cutlass.json.JsonLexer;
import io.questdb.client.cutlass.json.JsonParser;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class OidcDeviceAuthTest {

    private static final String DEVICE_PATH = "/device";
    private static final JsonParser NOOP_JSON_PARSER = (code, tag, position) -> {
    };
    private static final String SETTINGS_PATH = "/settings";
    private static final String TOKEN_PATH = "/token";

    @Test(timeout = 30_000)
    public void testAccessDeniedSurfacesOauthError() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"error\":\"access_denied\",\"error_description\":\"the user declined\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("access_denied", e.getOauthError());
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("the user declined"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAudienceParameterSentToDeviceEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            // the optional audience builder parameter must be url-encoded into the device authorization request
            AtomicReference<String> deviceBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceBody.set(body);
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-AUD", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .audience("api://questdb")
                         .allowInsecureTransport(true)
                         .prompt(noopPrompt())
                         .build()) {
                Assert.assertEquals("ACCESS-AUD", auth.getToken());
                Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("audience=api%3A%2F%2Fquestdb"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderRejectsMissingRequiredOptions() {
        try {
            OidcDeviceAuth.builder().deviceAuthorizationEndpoint("https://h/d").tokenEndpoint("https://h/t").build();
            Assert.fail("expected clientId validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("clientId"));
        }
        try {
            OidcDeviceAuth.builder().clientId("c").tokenEndpoint("https://h/t").build();
            Assert.fail("expected deviceAuthorizationEndpoint validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("deviceAuthorizationEndpoint"));
        }
        try {
            OidcDeviceAuth.builder().clientId("c").deviceAuthorizationEndpoint("https://h/d").build();
            Assert.fail("expected tokenEndpoint validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("tokenEndpoint"));
        }
    }

    @Test(timeout = 30_000)
    public void testChallengeStripsControlCharactersFromDisplayFields() throws Exception {
        assertMemoryLeak(() -> {
            // an attacker-influenced device-auth response embeds ANSI/control characters; the challenge
            // shown to the user must have them stripped so it cannot rewrite or spoof the terminal
            String evilUserCode = "WD\u001b[2JJB";                            // ESC clear-screen sequence
            String evilUri = "https://verify.example/\r\nFAKE: enter 000";    // CRLF line injection
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"" + evilUserCode + "\","
                            + "\"verification_uri\":\"" + evilUri + "\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-OK", null, null, 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-OK", auth.getToken());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                // the control characters are removed, the rest of the value is preserved
                Assert.assertEquals("WD[2JJB", challenge.getUserCode());
                Assert.assertEquals("https://verify.example/FAKE: enter 000", challenge.getVerificationUri());
                assertNoControlChars(challenge.getUserCode());
                assertNoControlChars(challenge.getVerificationUri());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testChunkedTokenResponseParses() throws Exception {
        assertMemoryLeak(() -> {
            // real IdPs use Transfer-Encoding: chunked; a multi-KB id token split across chunks must parse
            StringBuilder bigToken = new StringBuilder();
            for (int i = 0; i < 3000; i++) {
                bigToken.append('a');
            }
            String idToken = bigToken.toString();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.chunkedJson(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.chunkedJson(200, tokenJson("ACCESS-CHUNKED", idToken, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                // groups-in-token mode serves the id token; it arrived chunked and is 3 KB long
                Assert.assertEquals(idToken, auth.getToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClearCacheForcesFreshSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // clearCache() must drop the cached token AND the refresh token, so the next getToken() runs a
            // fresh interactive sign-in (a device-code grant) rather than a silent refresh
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshCalls.incrementAndGet();
                    return MockOidcServer.json(200, tokenJson("ACCESS-R", null, "REFRESH-R", 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                auth.clearCache();
                // the next call must run a second device-code sign-in, not a refresh (the refresh token was dropped)
                Assert.assertEquals("ACCESS-1", auth.getToken());
                Assert.assertEquals("clearCache must force a second interactive sign-in", 2, deviceCalls.get());
                Assert.assertEquals("clearCache must drop the refresh token so no refresh is attempted", 0, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClockSkewSecondsForcesEarlyRefresh() throws Exception {
        assertMemoryLeak(() -> {
            // a clock skew larger than the token lifetime makes a freshly-issued token count as already
            // expired, so the second getToken() refreshes instead of returning the cached token
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshCalls.incrementAndGet();
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 60));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .clockSkewSeconds(120) // larger than the 60s token lifetime
                         .allowInsecureTransport(true)
                         .prompt(noopPrompt())
                         .build()) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the 60s token sits within the 120s skew, so it is treated as expired and refreshed
                Assert.assertEquals("ACCESS-2", auth.getToken());
                Assert.assertEquals(1, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseCancelsInFlightSignIn() throws Exception {
        // a sign-in is waiting for the user: the token endpoint keeps returning authorization_pending.
        // close() from another caller must abort the in-flight getToken() promptly, instead of letting
        // it hold the instance lock and poll until the device code expires
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 10));
                }
                return MockOidcServer.json(400, "{\"error\":\"authorization_pending\"}");
            };
            CountDownLatch polling = new CountDownLatch(1);
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, challenge -> polling.countDown())) {
                Thread signIn = new Thread(() -> {
                    try {
                        auth.getToken();
                        outcome.set(new AssertionError("getToken() should have been cancelled by close()"));
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                }, "oidc-sign-in");
                signIn.setDaemon(true);
                signIn.start();
                // wait until the flow has prompted and is polling, then close from this thread
                Assert.assertTrue("the sign-in did not reach the polling stage", polling.await(10, TimeUnit.SECONDS));
                auth.close();
                signIn.join(10_000);
                Assert.assertFalse("getToken() did not return promptly after close()", signIn.isAlive());
                Throwable t = outcome.get();
                Assert.assertTrue("expected an OidcAuthException, got " + t, t instanceof OidcAuthException);
                Assert.assertTrue(t.getMessage(), t.getMessage().contains("closed"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConcurrentGetTokenStartsSingleSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // several callers race getToken() on a fresh instance; the synchronized method must serialize
            // them so exactly one interactive sign-in runs and the rest get the cached token
            AtomicInteger deviceCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-CONCURRENT", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                int workerCount = 4;
                CountDownLatch ready = new CountDownLatch(workerCount);
                CountDownLatch go = new CountDownLatch(1);
                AtomicReference<Throwable> error = new AtomicReference<>();
                String[] tokens = new String[workerCount];
                Thread[] workers = new Thread[workerCount];
                for (int i = 0; i < workerCount; i++) {
                    final int idx = i;
                    workers[i] = new Thread(() -> {
                        ready.countDown();
                        try {
                            go.await();
                            tokens[idx] = auth.getToken();
                        } catch (Throwable t) {
                            error.set(t);
                        }
                    }, "oidc-getToken-" + i);
                    workers[i].setDaemon(true);
                    workers[i].start();
                }
                Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                for (Thread w : workers) {
                    w.join(10_000);
                }
                Assert.assertNull("a worker failed: " + error.get(), error.get());
                Assert.assertEquals("only one interactive sign-in must run", 1, deviceCalls.get());
                for (int i = 0; i < workerCount; i++) {
                    Assert.assertEquals("ACCESS-CONCURRENT", tokens[i]);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDeviceEndpointReturnsOauthError() throws Exception {
        assertMemoryLeak(() -> {
            // the device authorization request itself is rejected (e.g. the client is not allowed)
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(400, "{\"error\":\"invalid_client\",\"error_description\":\"unknown client\"}");
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("invalid_client", e.getOauthError());
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("unknown client"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDeviceFlowHappyPath() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    Assert.assertTrue(body, body.contains("client_id=questdb"));
                    Assert.assertTrue(body, body.contains("scope=openid"));
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                // first poll: still pending, second poll: success
                Assert.assertTrue(body, body.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"));
                Assert.assertTrue(body, body.contains("device_code=DEV-CODE"));
                if (tokenCalls.getAndIncrement() == 0) {
                    return MockOidcServer.json(400, "{\"error\":\"authorization_pending\"}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", "ID-1", "REFRESH-1", 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                Assert.assertEquals("Bearer ACCESS-1", auth.getAuthorizationHeaderValue());
                Assert.assertEquals(2, tokenCalls.get());

                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                Assert.assertEquals("WDJB-MJHT", challenge.getUserCode());
                Assert.assertEquals("https://verify.example/device", challenge.getVerificationUri());
                Assert.assertEquals("https://verify.example/device?user_code=WDJB-MJHT", challenge.getVerificationUriComplete());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryDefaultsScopeToOpenid() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            AtomicReference<String> deviceBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    // settings advertise no scope, so the client must default to "openid"
                    return MockOidcServer.json(200, "{\"config\":{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                            + "}}");
                }
                if (DEVICE_PATH.equals(path)) {
                    deviceBody.set(body);
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-SCOPE", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true)) {
                    Assert.assertEquals("ACCESS-SCOPE", auth.getToken());
                    Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("scope=openid"));
                    Assert.assertFalse(deviceBody.get(), deviceBody.get().contains("groups"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryIgnoresPreferencesKeys() throws Exception {
        assertMemoryLeak(() -> {
            // the unprivileged-writable "preferences" object tries to poison discovery (flip enabled
            // off, flip groups-in-token, inject scope); only the trusted top-level "config" object
            // must feed discovery
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            AtomicReference<String> deviceBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"config\":{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.scope\":\"openid\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                            + "},\"preferences.version\":0,\"preferences\":{"
                            + "\"acl.oidc.enabled\":false,"
                            + "\"acl.oidc.groups.encoded.in.token\":true,"
                            + "\"acl.oidc.scope\":\"INJECTED\""
                            + "}}");
                }
                if (DEVICE_PATH.equals(path)) {
                    deviceBody.set(body);
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-TRUSTED", "ID-TRUSTED", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true)) {
                    // enabled stayed true (no DoS), groups-in-token stayed false (access token served),
                    // scope stayed "openid" (no injection)
                    Assert.assertEquals("ACCESS-TRUSTED", auth.getToken());
                    Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("scope=openid"));
                    Assert.assertFalse(deviceBody.get(), deviceBody.get().contains("INJECTED"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryRejectsMissingClientId() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                // OIDC enabled, endpoints advertised, but no client id
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("client id"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryRejectsMissingTokenEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                // OIDC enabled with a client id, but no token endpoint
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.client.id\":\"questdb\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("token endpoint"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryTransportFailureDoesNotLeakNativeMemory() throws Exception {
        // discoverSettings allocates a JSON lexer and an HTTP client and frees both in a finally; a transport
        // failure during discovery must not leak the lexer's native buffer. The module's assertMemoryLeak does
        // not flag single-tag growth, so measure the parser tag directly (as testMalformedEndpoint... does).
        int deadPort;
        try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            deadPort = probe.getLocalPort();
        } // closed now - nothing listens on deadPort
        long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
        try {
            OidcDeviceAuth.fromQuestDB("http://127.0.0.1:" + deadPort, true);
            Assert.fail("expected discovery to fail against a dead port");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not reach the QuestDB server"));
        }
        Assert.assertEquals("the discovery JSON lexer native buffer leaked",
                parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
    }

    @Test(timeout = 30_000)
    public void testDuplicateJsonKeysDoNotConcatenate() throws Exception {
        assertMemoryLeak(() -> {
            // a buggy/hostile IdP repeats a key; the parser must keep the last value, not concatenate it
            // onto the first (e.g. AAABBB), which would corrupt the served token and the device code
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV-CODE\","
                            + "\"user_code\":\"WRONG\",\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, "{\"token_type\":\"Bearer\",\"expires_in\":3600,"
                        + "\"access_token\":\"AAA\",\"access_token\":\"ACCESS-LAST\"}");
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                // the duplicate access_token resolves to the last value, not "AAAACCESS-LAST"
                Assert.assertEquals("ACCESS-LAST", auth.getToken());
                // the duplicate user_code resolves to the last value, not "WRONGWDJB-MJHT"
                Assert.assertEquals("WDJB-MJHT", shown.get().getUserCode());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testEndpointParseRejectsMalformedUrls() {
        // Endpoint.parse rejects malformed endpoint URLs at build time
        assertBuildFails("ftp://idp/d", "https://idp/t", "expected http or https");
        assertBuildFails("idp/d", "https://idp/t", "expected a scheme");
        assertBuildFails("https://idp/d", "https://idp:notaport/t", "could not parse the port");
        assertBuildFails("https:///d", "https://idp/t", "the host is empty");
        assertBuildFails("https://[::1]:9000/d", "https://idp/t", "IPv6 literal hosts are not supported");
    }

    @Test(timeout = 30_000)
    public void testEscapedDeviceCodeRoundTripsDecoded() throws Exception {
        assertMemoryLeak(() -> {
            // an IdP that escapes a character in device_code (here a slash) must have it decoded before the
            // client posts it back, otherwise the polled device_code never matches what the IdP issued
            AtomicReference<String> pollBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\\/CODE\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                pollBody.set(body);
                return MockOidcServer.json(200, tokenJson("ACCESS-DC", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-DC", auth.getToken());
                // device_code was "DEV\/CODE" in JSON; decoded to "DEV/CODE" and url-encoded as DEV%2FCODE
                Assert.assertTrue(pollBody.get(), pollBody.get().contains("device_code=DEV%2FCODE"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testEscapedErrorDescriptionDecoded() throws Exception {
        assertMemoryLeak(() -> {
            // an error_description with JSON-escaped characters must be decoded in the exception message,
            // not shown with literal backslashes
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"error\":\"access_denied\",\"error_description\":\"it\\\"s a \\/ test\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("access_denied", e.getOauthError());
                    // the escapes are decoded, not shown literally
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("it\"s a / test"));
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("\\/"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testEscapedVerificationUrlIsUnescapedForDisplay() throws Exception {
        assertMemoryLeak(() -> {
            // some identity providers JSON-escape forward slashes (PHP json_encode does by default), e.g.
            // "https:\/\/...". The challenge shown to the user must decode the escapes, not display literal
            // backslashes that break the link
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV-CODE\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https:\\/\\/verify.example\\/device\","
                            + "\"verification_uri_complete\":\"https:\\/\\/verify.example\\/device?user_code=WDJB-MJHT\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-ESC", null, null, 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-ESC", auth.getToken());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                Assert.assertEquals("https://verify.example/device", challenge.getVerificationUri());
                Assert.assertEquals("https://verify.example/device?user_code=WDJB-MJHT", challenge.getVerificationUriComplete());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbDiscoveryRunsFlow() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
                }
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-D", "ID-D", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true)) {
                    // discovery advertises groups.encoded.in.token=true, so getToken() must return the id token
                    Assert.assertEquals("ID-D", auth.getToken());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbRejectsInsecureServerUrl() {
        // the default-secure fromQuestDB overload must reject an http:// QuestDB server url (the discovery
        // response and the sign-in it bootstraps would travel in cleartext) unless insecure transport is
        // explicitly opted in
        try {
            OidcDeviceAuth.fromQuestDB("http://questdb.example:9000");
            Assert.fail("expected an http server url to be rejected");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("QuestDB server url"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
        }
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbRejectsMissingDeviceEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                // OIDC enabled, but no device authorization endpoint advertised (an older server)
                return MockOidcServer.json(200, settingsJson(true, false, server.httpUrl(TOKEN_PATH), null));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("device authorization endpoint"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbRejectsOidcDisabled() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(200, settingsJson(false, false, serverRef.get().httpUrl(TOKEN_PATH), null));
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("OIDC is not enabled"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGarbledRefreshResponseFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // the cached token expires and the refresh hits a transient non-JSON body (e.g. a gateway
            // 502 HTML page). The client must fall back to the interactive flow, not propagate the parse
            // failure out of getToken()
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // a transient gateway error page instead of a token JSON
                    return MockOidcServer.json(502, "<html>502 Bad Gateway</html>");
                }
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the cached token is expired vs the 30s skew, and the refresh body is garbled, so the
                // client must re-run the interactive flow instead of throwing the parse error
                Assert.assertEquals("ACCESS-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenSilentlyRefreshesWithoutPrompting() throws Exception {
        assertMemoryLeak(() -> {
            // getTokenSilently() returns the cached token, silently refreshes it when it expires, and never
            // prompts; if it cannot produce a token without an interactive sign-in, it throws
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger promptCalls = new AtomicInteger();
            AtomicBoolean refreshOk = new AtomicBoolean(true);
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    return refreshOk.get()
                            ? MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 1))
                            : MockOidcServer.json(400, "{\"error\":\"invalid_grant\"}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, ch -> promptCalls.incrementAndGet())) {
                // before any sign-in, getTokenSilently() must not prompt - it throws
                try {
                    auth.getTokenSilently();
                    Assert.fail("expected getTokenSilently() to fail before sign-in");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no token"));
                }
                // sign in once interactively
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the cached token is expired vs the 30s skew, so getTokenSilently() refreshes silently
                Assert.assertEquals("ACCESS-2", auth.getTokenSilently());
                // now make the refresh fail; getTokenSilently() must throw, not start the device flow
                refreshOk.set(false);
                try {
                    auth.getTokenSilently();
                    Assert.fail("expected getTokenSilently() to fail when the refresh is rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("interactive sign-in"));
                }
                // the device flow ran exactly once (the initial getToken), and the user was prompted once
                Assert.assertEquals(1, deviceCalls.get());
                Assert.assertEquals(1, promptCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGroupsInTokenButNoIdTokenFails() throws Exception {
        assertMemoryLeak(() -> {
            // groups encoded in token, but the IdP returns only an access token on the initial grant
            // (e.g. the requested scope omitted openid); getToken() must fail with an actionable message
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-ONLY", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no id_token"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGroupsInTokenReturnsIdToken() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                Assert.assertEquals("ID-X", auth.getToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testHttpSenderPullsTokenProviderPerRequest() throws Exception {
        assertMemoryLeak(() -> {
            // a long-lived HTTP Sender must pull the token from the provider on each request, so a rotating
            // token (as OidcDeviceAuth produces on refresh) reaches the wire without rebuilding the sender
            MockOidcServer.Handler handler = (method, path, body) -> MockOidcServer.json(204, "");
            AtomicInteger tokenSeq = new AtomicInteger();
            try (MockOidcServer server = new MockOidcServer(handler);
                 Sender sender = Sender.builder(Sender.Transport.HTTP)
                         .address("127.0.0.1:" + server.port())
                         .protocolVersion(Sender.PROTOCOL_VERSION_V2)
                         .httpTokenProvider(() -> "TOKEN-" + tokenSeq.incrementAndGet())
                         .build()) {
                sender.table("t").doubleColumn("x", 1.0).atNow();
                sender.flush();
                sender.table("t").doubleColumn("x", 2.0).atNow();
                sender.flush();
                // each flush built a fresh request and pulled a fresh token; the server saw successive bearers
                java.util.List<String> seen = server.requestAuthHeaders();
                Assert.assertTrue("expected at least 2 write requests, got " + seen, seen.size() >= 2);
                Assert.assertTrue(seen.toString(), seen.contains("Bearer TOKEN-1"));
                Assert.assertTrue(seen.toString(), seen.contains("Bearer TOKEN-2"));
                Assert.assertNotEquals("the token must rotate per request", seen.get(0), seen.get(1));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIncompleteDeviceResponseRejected() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint returns 200 but omits user_code and verification_uri
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(200, "{\"device_code\":\"DEV\",\"expires_in\":300,\"interval\":1}");
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("incomplete device authorization"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testInsecureEndpointsRejectedUnlessOptedIn() throws Exception {
        assertMemoryLeak(() -> {
            // http endpoints carry tokens in cleartext; the client must refuse them unless the caller opts in
            try {
                OidcDeviceAuth.builder()
                        .clientId("c")
                        .deviceAuthorizationEndpoint("http://idp.example/device")
                        .tokenEndpoint("https://idp.example/token")
                        .build();
                Assert.fail("expected the http device authorization endpoint to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("device authorization endpoint"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
            }
            try {
                OidcDeviceAuth.builder()
                        .clientId("c")
                        .deviceAuthorizationEndpoint("https://idp.example/device")
                        .tokenEndpoint("http://idp.example/token")
                        .build();
                Assert.fail("expected the http token endpoint to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("token endpoint"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
            }
            // opting in allows http, for local development
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://idp.example/device")
                    .tokenEndpoint("http://idp.example/token")
                    .allowInsecureTransport(true)
                    .build()
                    .close();
        });
    }

    @Test(timeout = 30_000)
    public void testLargeSplitTokenValueParsesWithConfiguredLexerSizing() throws Exception {
        assertMemoryLeak(() -> {
            // A real id_token (a JWT with group claims) runs to several KB, and a single JSON string value
            // can arrive split across HTTP response fragments. OidcDeviceAuth must size its JSON lexer so
            // such a split value still parses. This mirrors OidcDeviceAuth's production sizing
            // (JSON_LEXER_CACHE_SIZE / JSON_LEXER_MAX_VALUE_BYTES); the original (1024, 1024) sizing
            // rejected a >1024-byte split value with "String is too long".
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < 4000; i++) {
                value.append('a');
            }
            String json = "{\"id_token\":\"" + value + "\"}";
            int len = json.length();
            int split = "{\"id_token\":\"".length() + 1300; // boundary inside the value, past the old 1024 limit
            long address = TestUtils.toMemory(json);
            try {
                try {
                    parseSplitValue(1024, 1024, address, split, len);
                    Assert.fail("the original 1024-byte cache limit must reject a split multi-KB token value");
                } catch (JsonException expected) {
                    Assert.assertTrue(expected.getFlyweightMessage().toString(),
                            expected.getFlyweightMessage().toString().contains("String is too long"));
                }
                // the sizing OidcDeviceAuth now uses parses the same split value
                parseSplitValue(1024, 1 << 20, address, split, len);
            } finally {
                Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testMalformedEndpointDoesNotLeakNativeMemory() {
        // allowInsecureTransport skips build()'s own Endpoint.parse, so the constructor is the first to
        // parse and throw on this malformed url; the native JSON lexer must not have been allocated yet
        // (otherwise the never-returned instance leaks it). Measure the parser tag directly - the
        // module's assertMemoryLeak does not flag a single-tag growth.
        long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
        try {
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("not-a-url")
                    .tokenEndpoint("https://idp.example/token")
                    .allowInsecureTransport(true)
                    .build();
            Assert.fail("expected Endpoint.parse to reject the malformed url");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("expected a scheme"));
        }
        Assert.assertEquals("the JSON lexer native buffer leaked",
                parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
    }

    @Test(timeout = 30_000)
    public void testNoAccessTokenWhenGroupsDisabledFails() throws Exception {
        assertMemoryLeak(() -> {
            // groups not in token, but the IdP returns only an id token; getToken() must fail
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson(null, "ID-ONLY", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no access_token"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNullPromptDefaultsToSystemOut() throws Exception {
        assertMemoryLeak(() -> {
            // builder.prompt(null) must fall back to the default prompt rather than NPE during the flow
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-NP", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .prompt(null)
                         .allowInsecureTransport(true)
                         .build()) {
                // no NPE: the flow runs to completion with the default SYSTEM_OUT prompt
                Assert.assertEquals("ACCESS-NP", auth.getToken());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testOauthErrorMessageStripsControlChars() throws Exception {
        assertMemoryLeak(() -> {
            // an IdP error_description carrying ANSI/CRLF control chars must not reach the exception
            // message verbatim (it would let a malicious IdP rewrite the terminal or forge log lines)
            String desc = "denied" + ((char) 0x1b) + "[2J\r\nFAKE: paste your token";
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(400, "{\"error\":\"access_denied\",\"error_description\":\"" + desc + "\"}");
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("access_denied", e.getOauthError());
                    String msg = e.getMessage();
                    assertNoControlChars(msg);
                    Assert.assertTrue(msg, msg.contains("access_denied"));
                    Assert.assertTrue(msg, msg.contains("FAKE: paste your token")); // readable text survives
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testOutOfRangePollIntervalAndExpiryAreClamped() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or misconfigured identity provider reports an absurd interval/expires_in; the
            // client must clamp both, so interval*1000 cannot overflow into a zero-delay busy loop and
            // the wait cannot run absurdly long
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(2_000_000_000, 2_000_000_000));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-CLAMP", null, null, 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-CLAMP", auth.getToken());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                // the absurd interval/expires_in are clamped to the documented maxima
                Assert.assertTrue("interval=" + challenge.getIntervalSeconds(), challenge.getIntervalSeconds() <= 300);
                Assert.assertTrue("expiresIn=" + challenge.getExpiresInSeconds(), challenge.getExpiresInSeconds() <= 3600);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPersistentTransportFailureDuringPollingAborts() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint works, but the token endpoint is unreachable; polling must abort with
            // the underlying transport error after a few attempts, not retry silently until the code expires
            int deadPort;
            try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                deadPort = probe.getLocalPort();
            } // closed now - nothing listens on deadPort
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(200, deviceAuthorizationJson(1, 10));
            try (MockOidcServer server = new MockOidcServer(handler)) {
                try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                        .clientId("questdb")
                        .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                        .tokenEndpoint("http://127.0.0.1:" + deadPort + "/token")
                        .allowInsecureTransport(true)
                        .prompt(noopPrompt())
                        .build()) {
                    auth.getToken();
                    Assert.fail("expected a transport failure to abort polling");
                } catch (OidcAuthException e) {
                    // surfaces the transport failure, not the device-code-expired timeout
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("timed out"));
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("unreachable"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshErrorFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // the cached token expires and the refresh is rejected (revoked/expired refresh token);
            // the client must fall back to a fresh interactive sign-in
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    return MockOidcServer.json(400, "{\"error\":\"invalid_grant\"}");
                }
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the refresh is rejected, so the flow re-runs the interactive sign-in
                Assert.assertEquals("ACCESS-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshKeepsExistingRefreshTokenWhenOmitted() throws Exception {
        assertMemoryLeak(() -> {
            // a refresh response that omits refresh_token (RFC 6749 permits this) must not drop the existing
            // refresh token; a later refresh must reuse it rather than fall back to a fresh interactive sign-in
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // every refresh must present the ORIGINAL refresh token, and returns a short-lived
                    // access token WITHOUT a new refresh_token
                    Assert.assertTrue(body, body.contains("refresh_token=REFRESH-1"));
                    int n = refreshCalls.incrementAndGet();
                    return MockOidcServer.json(200, tokenJson("ACCESS-R" + n, null, null, 1));
                }
                // the initial device-code grant: a short-lived access token plus the refresh token
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // first refresh omits refresh_token, so REFRESH-1 must be kept
                Assert.assertEquals("ACCESS-R1", auth.getToken());
                // second refresh must still present the retained REFRESH-1 (asserted in the handler)
                Assert.assertEquals("ACCESS-R2", auth.getToken());
                Assert.assertEquals("no extra interactive sign-in", 1, deviceCalls.get());
                Assert.assertEquals(2, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshWithoutIdTokenFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // groups are encoded in the token (the default enterprise config), so getToken() serves the
            // id token. The cached token expires and the refresh response omits id_token (RFC 6749 makes
            // it optional on refresh), so the client must re-run the interactive flow rather than fail.
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // a refresh that returns a fresh access token but no id_token
                    return MockOidcServer.json(200, tokenJson("ACCESS-R", null, null, 3600));
                }
                // the device-code grant: first a soon-expired token, then (after fallback) a fresh one
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-1", "ID-1", "REFRESH-1", 1));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-2", "ID-2", "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                Assert.assertEquals("ID-1", auth.getToken());
                // the refresh returns no id_token, so the flow falls back to interactive sign-in and
                // returns the fresh id token instead of throwing "returned no id_token"
                Assert.assertEquals("ID-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerErrorDuringPollingRetries() throws Exception {
        assertMemoryLeak(() -> {
            // the token endpoint returns a gateway 5xx with an empty body once (no JSON error), then a
            // token. An empty-bodied upstream blip must be retried, not aborted as an "unexpected response"
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (tokenCalls.getAndIncrement() == 0) {
                    return MockOidcServer.json(502, "");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-RECOVERED-5XX", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-RECOVERED-5XX", auth.getToken());
                Assert.assertEquals(2, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSilentRefreshWhenTokenExpired() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger promptCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    Assert.assertTrue(body, body.contains("refresh_token=REFRESH-1"));
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", "ID-2", null, 3600));
                }
                // initial device-code grant, hand out a token that is already expired vs the clock skew
                return MockOidcServer.json(200, tokenJson("ACCESS-1", "ID-1", "REFRESH-1", 1));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, ch -> promptCalls.incrementAndGet())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the cached token is expired vs the 30s skew, so the second call refreshes silently
                Assert.assertEquals("ACCESS-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run only once", 1, deviceCalls.get());
                Assert.assertEquals("the user must be prompted only once", 1, promptCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSlowDownIncreasesIntervalAndSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger tokenCalls = new AtomicInteger();
            AtomicLong firstPollNanos = new AtomicLong();
            AtomicLong secondPollNanos = new AtomicLong();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                int call = tokenCalls.getAndIncrement();
                if (call == 0) {
                    firstPollNanos.set(System.nanoTime());
                    return MockOidcServer.json(400, "{\"error\":\"slow_down\"}");
                }
                if (call == 1) {
                    secondPollNanos.set(System.nanoTime());
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-S", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-S", auth.getToken());
                Assert.assertEquals(2, tokenCalls.get());
                // base interval is 1s; the slow_down must add ~5s, so the SECOND poll lands ~6s after
                // the first. Assert the inter-poll gap directly, not just total elapsed - without the
                // increment the gap would be ~1s.
                long gapMillis = (secondPollNanos.get() - firstPollNanos.get()) / 1_000_000L;
                Assert.assertTrue("inter-poll gap=" + gapMillis + "ms", gapMillis >= 4_000);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testStalledResponseBodyAbortsWithinTimeout() throws Exception {
        assertMemoryLeak(() -> {
            // a server that sends headers then stalls the body must not wedge the thread on the 10-minute
            // HttpClient default timeout; the body read aborts on the configured OIDC timeout instead
            MockOidcServer.Handler handler = (method, path, body) -> MockOidcServer.stall();
            try (MockOidcServer server = new MockOidcServer(handler)) {
                long startNanos = System.nanoTime();
                try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                        .clientId("questdb")
                        .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                        .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                        .httpTimeoutMillis(1_000)
                        .allowInsecureTransport(true)
                        .prompt(noopPrompt())
                        .build()) {
                    auth.getToken();
                    Assert.fail("expected the stalled body read to abort");
                } catch (OidcAuthException e) {
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    // aborted on the ~1s OIDC timeout, not the 600s HttpClient default (or an indefinite wedge)
                    Assert.assertTrue("aborted too slowly: " + elapsedMillis + "ms", elapsedMillis < 10_000);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTimesOutWhenCodeExpires() throws Exception {
        assertMemoryLeak(() -> {
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    // very short lifetime so the poll loop gives up quickly
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 1));
                }
                return MockOidcServer.json(400, "{\"error\":\"authorization_pending\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected a timeout");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenCachedAcrossCalls() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                tokenCalls.incrementAndGet();
                return MockOidcServer.json(200, tokenJson("ACCESS-C", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-C", auth.getToken());
                Assert.assertEquals("ACCESS-C", auth.getToken());
                Assert.assertEquals("ACCESS-C", auth.getToken());
                Assert.assertEquals("the interactive flow must run only once", 1, deviceCalls.get());
                Assert.assertEquals("the token endpoint must be hit only once", 1, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenEndpointErrorDoesNotLeakSecretsInMessage() throws Exception {
        assertMemoryLeak(() -> {
            final String secret = "SUPER-SECRET-TOKEN-VALUE-0123456789";
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                // a 200 that carries a token but is malformed JSON: the parser fails, and the raw body
                // (with the token) must NOT be echoed into the exception message
                return MockOidcServer.json(200, "{\"access_token\":\"" + secret + "\" not-valid-json}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertFalse("the token must not leak into the message: " + e.getMessage(),
                            e.getMessage().contains(secret));
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("httpStatus="));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTransientParseFailureDuringPollingRecovers() throws Exception {
        assertMemoryLeak(() -> {
            // the token endpoint returns a garbled (non-JSON) body once, then a valid token; a transient
            // parse failure is retried like a transport blip rather than aborting the sign-in
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (tokenCalls.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, "<html>502 Bad Gateway</html>");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-RECOVERED", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-RECOVERED", auth.getToken());
                Assert.assertEquals(2, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTruncatedSettingsResponseRejected() throws Exception {
        assertMemoryLeak(() -> {
            // the /settings body is cut off mid-object (HTTP framing satisfied, JSON unterminated). discovery
            // must reject it as a parse failure, not silently discover from the partial document and report a
            // misleading "does not advertise ..." error
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(200, "{\"config\":{\"acl.oidc.enabled\":true,\"acl.oidc.client.id\":\"questdb\"");
            try (MockOidcServer server = new MockOidcServer(handler)) {
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to reject the truncated settings body");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not parse"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTruncatedTokenResponseRejected() throws Exception {
        assertMemoryLeak(() -> {
            // a token response whose Content-Length is satisfied but whose JSON is unterminated must be
            // rejected (parseLast catches the dangling value), not silently treated as no token
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, "{\"access_token\":\"abc");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not parse"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUnexpectedTokenResponseRejected() throws Exception {
        assertMemoryLeak(() -> {
            // the token endpoint returns 200 with neither tokens nor an error
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, "{\"token_type\":\"Bearer\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("unexpected response"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUnreachableDeviceEndpointThrowsOidcAuthException() throws Exception {
        assertMemoryLeak(() -> {
            // a connection failure to the device endpoint must surface as OidcAuthException (getToken's
            // documented failure type), not a raw HttpClientException
            int deadPort;
            try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                deadPort = probe.getLocalPort();
            } // closed now - nothing listens on deadPort
            try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                    .clientId("questdb")
                    .deviceAuthorizationEndpoint("http://127.0.0.1:" + deadPort + "/device")
                    .tokenEndpoint("http://127.0.0.1:" + deadPort + "/token")
                    .allowInsecureTransport(true)
                    .prompt(noopPrompt())
                    .build()) {
                auth.getToken();
                Assert.fail("expected an OidcAuthException");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("device authorization endpoint"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUseAfterCloseThrowsClearly() {
        // calling getToken()/clearCache() after close() must fail with a clear "closed" error rather than
        // NPE on the freed JSON lexer or resurrect (and leak) a fresh native HTTP client
        long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
        OidcDeviceAuth auth = OidcDeviceAuth.builder()
                .clientId("c")
                .deviceAuthorizationEndpoint("https://idp.example/device")
                .tokenEndpoint("https://idp.example/token")
                .build();
        auth.close();
        try {
            auth.getToken();
            Assert.fail("expected getToken() after close() to be rejected");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }
        try {
            auth.clearCache();
            Assert.fail("expected clearCache() after close() to be rejected");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }
        // getToken() must reject before resurrecting a native HTTP client, and close() must have freed
        // the JSON lexer, so the parser-tag memory returns to its pre-construction level
        Assert.assertEquals("a closed instance must not leak or resurrect native memory",
                parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
    }

    @Test(timeout = 30_000)
    public void testVerificationUrlAliasesParsed() throws Exception {
        assertMemoryLeak(() -> {
            // some identity providers (historically Google) return verification_url / verification_url_complete
            // instead of the RFC 8628 verification_uri / verification_uri_complete; both spellings must populate
            // the challenge shown to the user
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV-CODE\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_url\":\"https://verify.example/device\","
                            + "\"verification_url_complete\":\"https://verify.example/device?user_code=WDJB-MJHT\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-ALIAS", null, null, 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-ALIAS", auth.getToken());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                Assert.assertEquals("https://verify.example/device", challenge.getVerificationUri());
                Assert.assertEquals("https://verify.example/device?user_code=WDJB-MJHT", challenge.getVerificationUriComplete());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testWrongTokenKindDoesNotWedgeCache() throws Exception {
        assertMemoryLeak(() -> {
            // groups-in-token mode, but the IdP returns only an access token on the first grant (e.g. the
            // requested scope omitted openid). getToken() must fail the first call, then re-run the
            // interactive flow on the next call - not cache the unusable access token as valid and keep
            // throwing "no id_token" on every later call
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                // first grant: access token only (no id_token); second grant: a proper id token
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-ONLY", null, null, 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-2", "ID-2", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected an OidcAuthException on the first call");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no id_token"));
                }
                // the unusable grant must NOT be cached as valid: the next call re-runs the flow and succeeds
                Assert.assertEquals("ID-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run twice (failed first, recovered second)", 2, deviceCalls.get());
            }
        });
    }

    private static void assertBuildFails(String deviceEndpoint, String tokenEndpoint, String expectedMessage) {
        try {
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint(deviceEndpoint)
                    .tokenEndpoint(tokenEndpoint)
                    .build();
            Assert.fail("expected build to fail for device=" + deviceEndpoint + " token=" + tokenEndpoint);
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private static void assertNoControlChars(String value) {
        for (int i = 0; i < value.length(); i++) {
            Assert.assertFalse("control char at index " + i + " in '" + value + "'", Character.isISOControl(value.charAt(i)));
        }
    }

    private static String deviceAuthorizationJson(int interval, int expiresIn) {
        return "{"
                + "\"device_code\":\"DEV-CODE\","
                + "\"user_code\":\"WDJB-MJHT\","
                + "\"verification_uri\":\"https://verify.example/device\","
                + "\"verification_uri_complete\":\"https://verify.example/device?user_code=WDJB-MJHT\","
                + "\"expires_in\":" + expiresIn + ","
                + "\"interval\":" + interval
                + "}";
    }

    private static OidcDeviceAuth newAuth(MockOidcServer server, boolean groupsInToken, DeviceCodePrompt prompt) {
        return OidcDeviceAuth.builder()
                .clientId("questdb")
                .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                .scope("openid groups")
                .groupsInToken(groupsInToken)
                .prompt(prompt)
                .allowInsecureTransport(true)
                .build();
    }

    private static DeviceCodePrompt noopPrompt() {
        return challenge -> {
        };
    }

    private static void parseSplitValue(int cacheSize, int cacheSizeLimit, long address, int split, int len) throws JsonException {
        try (JsonLexer lexer = new JsonLexer(cacheSize, cacheSizeLimit)) {
            lexer.parse(address, address + split, NOOP_JSON_PARSER);
            lexer.parse(address + split, address + len, NOOP_JSON_PARSER);
            lexer.parseLast();
        }
    }

    private static String settingsJson(boolean enabled, boolean withDeviceEndpoint, String tokenEndpoint, String deviceEndpoint) {
        StringSink config = new StringSink();
        config.put("{\"config\":{");
        config.put("\"acl.oidc.enabled\":").put(Boolean.toString(enabled)).put(',');
        config.put("\"acl.oidc.client.id\":\"questdb\",");
        config.put("\"acl.oidc.scope\":\"openid groups\",");
        config.put("\"acl.oidc.groups.encoded.in.token\":true,");
        config.put("\"acl.oidc.token.endpoint\":\"").put(tokenEndpoint).put('"');
        if (withDeviceEndpoint) {
            config.put(",\"acl.oidc.device.authorization.endpoint\":\"").put(deviceEndpoint).put('"');
        }
        config.put("},\"preferences.version\":0,\"preferences\":{}}");
        return config.toString();
    }

    private static String tokenJson(String accessToken, String idToken, String refreshToken, int expiresIn) {
        StringSink sb = new StringSink();
        sb.put("{\"token_type\":\"Bearer\",\"expires_in\":").put(expiresIn);
        if (accessToken != null) {
            sb.put(",\"access_token\":\"").put(accessToken).put('"');
        }
        if (idToken != null) {
            sb.put(",\"id_token\":\"").put(idToken).put('"');
        }
        if (refreshToken != null) {
            sb.put(",\"refresh_token\":\"").put(refreshToken).put('"');
        }
        sb.put('}');
        return sb.toString();
    }
}
