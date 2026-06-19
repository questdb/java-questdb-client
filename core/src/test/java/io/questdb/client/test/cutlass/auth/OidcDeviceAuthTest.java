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

import java.lang.reflect.Method;
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
    private static final String WELL_KNOWN_PATH = "/.well-known/openid-configuration";

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
    public void testBuilderIssuerPinAcceptsMatchingOrigin() throws Exception {
        assertMemoryLeak(() -> {
            // endpoints that belong to the pinned issuer origin are accepted; only the origin is pinned, so
            // the differing paths of the device and token endpoints are fine
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://idp.example/as/device")
                    .tokenEndpoint("https://idp.example/as/token")
                    .issuer("https://idp.example")
                    .build()
                    .close();
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderIssuerPinRejectsOffOriginEndpoints() {
        // the token/device endpoints do not belong to the pinned issuer origin; build() must reject them
        // rather than send the device code and refresh token outside the trusted issuer
        try {
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://idp.example/device")
                    .tokenEndpoint("https://idp.example/token")
                    .issuer("https://other-idp.example")
                    .build();
            Assert.fail("expected the issuer pin to reject off-origin endpoints");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not match the issuer origin"));
        }
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
    public void testBuilderRejectsSplitOriginEndpoints() {
        // the token and device authorization endpoints are on different origins; RFC 8628 co-locates them
        // on one authorization server, so build() must refuse to spread the credential POSTs across hosts
        try {
            OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://device.example/device")
                    .tokenEndpoint("https://token.example/token")
                    .build();
            Assert.fail("expected split-origin endpoints to be rejected");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("different origins"));
        }
    }

    @Test(timeout = 30_000)
    public void testChallengeStripsBidiAndZeroWidthFromDisplayFields() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or MITM'd IdP smuggles bidi/zero-width formatting into the display fields. Here a
            // right-to-left override (U+202E) arrives as a JSON unicode escape, which this client's lexer
            // decodes into the real character before it reaches the prompt; a BOM, a zero-width space and a
            // bidi isolate arrive the same way. The challenge shown to the user must strip them all, so the
            // verification URL a human reads matches the one their browser opens
            String evilUri = "https://verify.example/" + jsonUnicodeEscape(0x202E) + "evil";           // RTL override
            String evilComplete = "https://verify.example/" + jsonUnicodeEscape(0xFEFF) + "device?x=1"; // BOM
            String evilUserCode = "W" + jsonUnicodeEscape(0x200B) + "D" + jsonUnicodeEscape(0x2066) + "JB"; // ZWSP + LRI
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"" + evilUserCode + "\","
                            + "\"verification_uri\":\"" + evilUri + "\","
                            + "\"verification_uri_complete\":\"" + evilComplete + "\","
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
                // the bidi/zero-width/BOM characters are removed, the readable text is preserved
                Assert.assertEquals("https://verify.example/evil", challenge.getVerificationUri());
                Assert.assertEquals("https://verify.example/device?x=1", challenge.getVerificationUriComplete());
                Assert.assertEquals("WDJB", challenge.getUserCode());
                assertNoUnsafeDisplayChars(challenge.getUserCode());
                assertNoUnsafeDisplayChars(challenge.getVerificationUri());
                assertNoUnsafeDisplayChars(challenge.getVerificationUriComplete());
            }
        });
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
    public void testChallengeStripsLoneSurrogates() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile IdP smuggles unpaired UTF-16 surrogates into display fields via single backslash-u-XXXX escapes
            // the lexer emits verbatim (it does not pair them). codePointAt surfaces a lone surrogate as a
            // SURROGATE code point, which the sanitizer must strip - while a legitimate adjacent high+low pair
            // (an emoji) that codePointAt reassembles survives.
            String loneHigh = jsonUnicodeEscape(0xD83D);                          // high surrogate, no low half
            String loneLow = jsonUnicodeEscape(0xDE00);                           // low surrogate, no high half
            String emoji = jsonUnicodeEscape(0xD83D) + jsonUnicodeEscape(0xDE00); // U+1F600, a valid pair
            String evilUserCode = "WD" + loneHigh + "JB";
            String evilUri = "https://verify.example/" + loneLow + "evil" + emoji;
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
                // the unpaired surrogates are removed; the readable text and the legitimate emoji survive
                Assert.assertEquals("WDJB", challenge.getUserCode());
                Assert.assertEquals("https://verify.example/evil" + new String(Character.toChars(0x1F600)),
                        challenge.getVerificationUri());
                assertNoUnsafeDisplayChars(challenge.getUserCode());
                assertNoUnsafeDisplayChars(challenge.getVerificationUri());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testChallengeStripsSupplementaryPlaneFormatChars() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile IdP smuggles a supplementary-plane (>= U+10000) format char - U+E0001 LANGUAGE TAG,
            // an invisible Unicode "tag" character (category Cf) used to hide or spoof text - via a
            // surrogate-pair JSON unicode escape the lexer reassembles. A per-UTF-16-unit filter misses it
            // (each surrogate half is neither a control nor Cf); the sanitizer must judge it per code point
            // and strip it, while leaving a legitimate astral character (an emoji) intact.
            String evilTag = jsonUnicodeEscape(0xDB40) + jsonUnicodeEscape(0xDC01); // U+E0001 as a surrogate pair
            String emoji = jsonUnicodeEscape(0xD83D) + jsonUnicodeEscape(0xDE00);   // U+1F600 grinning face
            String evilUserCode = "WD" + evilTag + "JB";
            String evilUri = "https://verify.example/" + evilTag + "evil" + emoji;
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
                // the invisible tag char is removed; the readable text and the legitimate emoji survive
                Assert.assertEquals("WDJB", challenge.getUserCode());
                Assert.assertEquals("https://verify.example/evil" + new String(Character.toChars(0x1F600)),
                        challenge.getVerificationUri());
                assertNoUnsafeDisplayChars(challenge.getUserCode());
                assertNoUnsafeDisplayChars(challenge.getVerificationUri());
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
        // discoverSettings allocates a JSON lexer (NATIVE_TEXT_PARSER_RSS) and an HTTP client (NATIVE_DEFAULT
        // buffers) and frees both in a finally; a transport failure during discovery must not leak either.
        // The module's assertMemoryLeak does not reliably flag single-tag growth, so measure both tags
        // directly. Measuring only the parser tag (as an earlier version did) was blind to a leak of the
        // HTTP client's native buffers - the resource most likely to be left dangling on the failure path.
        int deadPort;
        try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            deadPort = probe.getLocalPort();
        } // closed now - nothing listens on deadPort
        long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
        long clientMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        try {
            OidcDeviceAuth.fromQuestDB("http://127.0.0.1:" + deadPort, true);
            Assert.fail("expected discovery to fail against a dead port");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not reach the QuestDB server"));
        }
        Assert.assertEquals("the discovery JSON lexer native buffer leaked",
                parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
        Assert.assertEquals("the discovery HTTP client native buffers leaked",
                clientMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
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
    public void testEndpointParseRejectsDisplayUnsafeUrl() {
        // a url carrying a display-unsafe character is rejected, and the rejection message itself must carry
        // none: otherwise a tampered /settings or discovery endpoint url could reorder, hide or forge the
        // log line / exception text it lands in. The control-char scan alone does not catch these higher
        // code points (bidi, zero-width, BOM, supplementary-plane tag chars), the last scanned per code point
        String[] unsafe = {
                String.valueOf((char) 0x202E),          // right-to-left override
                String.valueOf((char) 0x200B),          // zero-width space
                String.valueOf((char) 0xFEFF),          // BOM / zero-width no-break space
                new String(Character.toChars(0xE0001))  // U+E0001 LANGUAGE TAG (supplementary-plane format char)
        };
        for (int i = 0; i < unsafe.length; i++) {
            String marker = unsafe[i];
            try {
                OidcDeviceAuth.builder()
                        .clientId("c")
                        .deviceAuthorizationEndpoint("https://idp.example/dev" + marker + "ice")
                        .tokenEndpoint("https://idp.example/t")
                        .build();
                Assert.fail("expected the display-unsafe url to be rejected [index=" + i + "]");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("illegal character"));
                // the raw unsafe character must not survive into the message
                assertNoUnsafeDisplayChars(e.getMessage());
            }
        }
    }

    @Test(timeout = 30_000)
    public void testEndpointParseRejectsMalformedUrls() {
        // Endpoint.parse rejects malformed endpoint URLs at build time
        assertBuildFails("ftp://idp/d", "https://idp/t", "expected http or https");
        assertBuildFails("idp/d", "https://idp/t", "expected a scheme");
        assertBuildFails("https://idp/d", "https://idp:notaport/t", "could not parse the port");
        assertBuildFails("https:///d", "https://idp/t", "the host is empty");
        assertBuildFails("https://[::1]:9000/d", "https://idp/t", "IPv6 literal hosts are not supported");
        // an out-of-range port (0, negative, or above 65535) is rejected rather than passed to the transport
        assertBuildFails("https://idp:99999/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp:0/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp:-1/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp/d", "https://idp:70000/t", "between 1 and 65535");
        // a host carrying control characters or whitespace (e.g. a smuggled CR/LF that would inject into the
        // outbound Host header) is rejected rather than passed verbatim to the transport
        assertBuildFails("https://ho\r\nst/d", "https://idp/t", "illegal character");
        assertBuildFails("https://h\tst/d", "https://idp/t", "illegal character");
        assertBuildFails("https://h st/d", "https://idp/t", "illegal character");
        assertBuildFails("https://idp/d", "https://e\nvil/t", "illegal character");
        // a control character or whitespace in the path or query is rejected too: postForm sends the path
        // verbatim on the request line, so a smuggled CR/LF there would inject a header / smuggle a request
        assertBuildFails("https://idp/devic\r\ne", "https://idp/t", "illegal character");
        assertBuildFails("https://idp/d", "https://idp/toke\r\nX-Injected:1", "illegal character");
        assertBuildFails("https://idp/d", "https://idp/t?a=b\nc", "illegal character");
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
    public void testFromQuestDbDiscoversDeviceEndpointFromIssuer() throws Exception {
        assertMemoryLeak(() -> {
            // the server advertises a token endpoint but not the device authorization endpoint (today's
            // servers); pinning the issuer lets the client discover the device endpoint from the issuer's
            // .well-known/openid-configuration document and complete the flow
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, false, server.httpUrl(TOKEN_PATH), null));
                }
                if (WELL_KNOWN_PATH.equals(path)) {
                    return MockOidcServer.json(200, wellKnownJson(server.httpUrl(DEVICE_PATH), server.httpUrl(TOKEN_PATH), server.httpUrl("")));
                }
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-WK", "ID-WK", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                // the issuer is the mock itself, which also serves the .well-known document and the IdP endpoints
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), server.httpUrl(""), true)) {
                    // settings advertise groups.encoded.in.token=true, so getToken() returns the id token
                    Assert.assertEquals("ID-WK", auth.getToken());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbDiscoversFromDiscoveryUrl() throws Exception {
        assertMemoryLeak(() -> {
            // a discovery url pins the identity provider directly (an alternative to an issuer); the device
            // endpoint and the issuer to pin against both come from the discovery document
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, false, server.httpUrl(TOKEN_PATH), null));
                }
                if (WELL_KNOWN_PATH.equals(path)) {
                    return MockOidcServer.json(200, wellKnownJson(server.httpUrl(DEVICE_PATH), server.httpUrl(TOKEN_PATH), server.httpUrl("")));
                }
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-DU", "ID-DU", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), null, server.httpUrl(WELL_KNOWN_PATH), null, true)) {
                    Assert.assertEquals("ID-DU", auth.getToken());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbDiscoveryDocMissingDeviceEndpointRejected() throws Exception {
        assertMemoryLeak(() -> {
            // discovery runs against the pinned issuer, but the discovery document does not advertise a
            // device authorization endpoint (the identity provider lacks the device grant); fail clearly
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, false, server.httpUrl(TOKEN_PATH), null));
                }
                // a discovery document with a token endpoint and issuer but no device_authorization_endpoint
                return MockOidcServer.json(200, "{"
                        + "\"issuer\":\"" + server.httpUrl("") + "\","
                        + "\"token_endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\""
                        + "}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), server.httpUrl(""), true);
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("device_authorization_endpoint"));
                }
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
    public void testFromQuestDbDiscoveryUrlPinAcceptsOnOriginAdvertisedEndpoints() throws Exception {
        assertMemoryLeak(() -> {
            // /settings advertises both endpoints on the same origin as the pinned discoveryUrl, so the pin
            // is satisfied and the flow completes - and without a discovery round-trip, since the discovery
            // branch is skipped when both endpoints are already advertised
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            AtomicBoolean wellKnownHit = new AtomicBoolean(false);
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
                }
                if (WELL_KNOWN_PATH.equals(path)) {
                    wellKnownHit.set(true);
                    return MockOidcServer.json(200, wellKnownJson(server.httpUrl(DEVICE_PATH), server.httpUrl(TOKEN_PATH), server.httpUrl("")));
                }
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-DUP", "ID-DUP", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), null, server.httpUrl(WELL_KNOWN_PATH), null, true)) {
                    Assert.assertEquals("ID-DUP", auth.getToken());
                }
                Assert.assertFalse("discovery must be skipped when /settings advertises both endpoints", wellKnownHit.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbDiscoveryUrlPinRejectsOffOriginAdvertisedEndpoints() throws Exception {
        assertMemoryLeak(() -> {
            // /settings advertises both endpoints directly (so the discovery branch is skipped), but they do
            // not belong to the pinned discoveryUrl origin; the discoveryUrl pin must reject them just as an
            // issuer pin does, rather than let a compromised server redirect the sign-in to its chosen origin
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), null, "https://trusted-idp.example/.well-known/openid-configuration", null, true);
                    Assert.fail("expected the discoveryUrl pin to reject the off-origin endpoints");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not match the issuer origin"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbIssuerPinRejectsOffOriginAdvertisedEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            // the server advertises both endpoints directly, but they do not belong to the pinned issuer
            // origin; the issuer pin must reject them rather than route credentials off the trusted issuer
            // (this is the protection against a compromised-but-reachable server redirecting the sign-in)
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), "https://idp.attacker.example", true);
                    Assert.fail("expected the issuer pin to reject the off-origin endpoints");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not match the issuer origin"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbRejectsCrlfInjectedAdvertisedEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            // a tampered /settings advertises a token endpoint whose path carries a JSON-escaped CR/LF; the
            // lexer decodes it to real control characters, and Endpoint.parse must reject it rather than let
            // it inject into the outbound request line (header smuggling against the identity provider)
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                String crlf = jsonUnicodeEscape(0x0d) + jsonUnicodeEscape(0x0a);
                String injectedToken = server.httpUrl(TOKEN_PATH) + crlf + "X-Injected:1";
                return MockOidcServer.json(200, settingsJson(true, true, injectedToken, server.httpUrl(DEVICE_PATH)));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected the CR/LF-injected token endpoint to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("illegal character"));
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
    public void testGetTokenSilentlyDoesNotBlockBehindInteractiveSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // an interactive getToken() is parked polling (authorization_pending), holding the instance
            // lock for the whole device-code lifetime. A flush-path getTokenSilently() on another thread
            // must NOT block behind it - it must fail fast, so a Sender flush is never stalled by a
            // concurrent sign-in. (With the old synchronized model it blocked until the code expired.)
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 10));
                }
                return MockOidcServer.json(400, "{\"error\":\"authorization_pending\"}");
            };
            CountDownLatch polling = new CountDownLatch(1);
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, challenge -> polling.countDown())) {
                Thread signIn = new Thread(() -> {
                    try {
                        auth.getToken();
                    } catch (Throwable ignore) {
                        // expected: cancelled by close() at the end of the test
                    }
                }, "oidc-sign-in");
                signIn.setDaemon(true);
                signIn.start();
                try {
                    // wait until the interactive flow has prompted and is polling (it holds the lock now)
                    Assert.assertTrue("the sign-in did not reach the polling stage", polling.await(10, TimeUnit.SECONDS));
                    // getTokenSilently() must return control promptly (here: throw), NOT block ~10s until
                    // the device code expires and getToken() releases the lock
                    long startNanos = System.nanoTime();
                    try {
                        auth.getTokenSilently();
                        Assert.fail("expected getTokenSilently() to fail fast while a sign-in is in progress");
                    } catch (OidcAuthException e) {
                        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        Assert.assertTrue("getTokenSilently() blocked " + elapsedMillis + "ms behind the in-flight sign-in",
                                elapsedMillis < 2_000);
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("in progress"));
                    }
                } finally {
                    auth.close();        // cancel the in-flight sign-in
                    signIn.join(10_000); // let the daemon thread unwind before the leak check
                }
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
    public void testHttpSenderProviderFailureAfterFlushDoesNotCorruptSender() throws Exception {
        assertMemoryLeak(() -> {
            // regression: the per-request token must be pulled lazily when a row starts, never eagerly when
            // the post-flush request is rebuilt. A provider that throws on a later pull (e.g.
            // OidcDeviceAuth::getTokenSilently when a refresh fails) must NOT turn an already-successful
            // flush into a thrown exception, and must NOT leave a half-built request that corrupts the
            // sender so later rows go out malformed
            MockOidcServer.Handler handler = (method, path, body) -> MockOidcServer.json(204, "");
            AtomicInteger pulls = new AtomicInteger();
            try (MockOidcServer server = new MockOidcServer(handler);
                 Sender sender = Sender.builder(Sender.Transport.HTTP)
                         .address("127.0.0.1:" + server.port())
                         .protocolVersion(Sender.PROTOCOL_VERSION_V2)
                         .httpTokenProvider(() -> {
                             int n = pulls.incrementAndGet();
                             if (n == 2) {
                                 // the second pull - for the request after the first, successful flush - fails
                                 throw new OidcAuthException("the cached token expired and could not be refreshed");
                             }
                             return "TOKEN-" + n;
                         })
                         .build()) {
                // first batch: the token is pulled when the row starts (TOKEN-1); the flush sends it and must
                // succeed. The failing *next* pull must not strike here - the eager post-flush pull was the bug
                sender.table("t").doubleColumn("x", 1.0).atNow();
                sender.flush();

                // next batch: the deferred pull runs when the row starts and the provider throws there; the
                // failure must surface cleanly, leaving the previous successful flush and its data untouched
                try {
                    sender.table("t").doubleColumn("x", 2.0).atNow();
                    Assert.fail("expected the failing provider pull to surface on the next row");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not be refreshed"));
                }

                // the provider recovers (pull #3 -> TOKEN-3); the failed pull must not have corrupted the
                // sender, so this row produces a well-formed request the server accepts
                sender.table("t").doubleColumn("x", 3.0).atNow();
                sender.flush();

                java.util.List<String> seen = server.requestAuthHeaders();
                Assert.assertTrue(seen.toString(), seen.contains("Bearer TOKEN-1"));
                Assert.assertTrue(seen.toString(), seen.contains("Bearer TOKEN-3"));
                // the failed pull never reached the wire as a partial request
                Assert.assertFalse(seen.toString(), seen.contains("Bearer TOKEN-2"));
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
    public void testLoopbackHostClassifierAcceptsLoopbackForms() throws Exception {
        // localhost (any case) and the whole 127.0.0.0/8 block are loopback: a plaintext /settings fetch to
        // them never leaves the host, so settingsChannelIsPlaintext correctly skips the plaintext-channel
        // pin. This is the pin's only exercised exemption, since MockOidcServer binds to loopback.
        String[] loopback = {
                "localhost", "LOCALHOST", "LocalHost",
                "127.0.0.1", "127.0.0.0", "127.1.2.3", "127.255.255.255", "127.0.0.255"
        };
        for (int i = 0; i < loopback.length; i++) {
            Assert.assertTrue("expected loopback: [" + loopback[i] + "]", invokeIsLoopbackHost(loopback[i]));
        }
    }

    @Test(timeout = 30_000)
    public void testLoopbackHostClassifierRejectsNonLoopbackAndSpoofing() throws Exception {
        // every other host must classify as non-loopback so the plaintext-channel MITM pin FIRES over http -
        // the firing path the loopback-bound test mock cannot reach end to end. A classifier that accepted
        // any of these as loopback would silently disable the pin for a tampered /settings endpoint.
        String[] notLoopback = {
                null, "",
                "example.com", "questdb.example",
                "127.evil.com",        // starts with "127." but is not a dotted-IPv4 literal
                "localhost.evil.com",  // not an exact localhost match
                "evil.localhost",
                "0x7f.0.0.1",          // hex form is not the dotted 127.0.0.0/8 literal
                "127.1", "127.0.1", "127", // short forms the OS would expand are deliberately not accepted
                "127.0.0.256",         // octet out of range
                "127.0.0.1.evil.com",  // extra label after a valid prefix
                "127.0.0.1.",          // trailing dot
                "127..0.1",            // empty octet
                "1270.0.0.1",          // does not start with "127."
                "227.0.0.1",           // not the 127 block
                "0.0.0.0", "10.0.0.1", "192.168.0.1", "::1"
        };
        for (int i = 0; i < notLoopback.length; i++) {
            Assert.assertFalse("expected non-loopback: [" + notLoopback[i] + "]", invokeIsLoopbackHost(notLoopback[i]));
        }
    }

    @Test(timeout = 30_000)
    public void testMalformedEndpointDoesNotLeakNativeMemory() {
        // build() parses the endpoints up front (for the co-location / issuer-pin checks) and throws on
        // this malformed url before the constructor allocates the native JSON lexer, so the never-returned
        // instance cannot leak it. Measure the parser tag directly - the module's assertMemoryLeak does not
        // flag a single-tag growth.
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
    public void testNonSuccessDeviceAuthorizationResponseRejected() throws Exception {
        assertMemoryLeak(() -> {
            // RFC 8628 3.2: a device authorization grant is a 2xx response. A non-2xx body that nonetheless
            // carries device_code/user_code/verification_uri and no OAuth error must be rejected - the client
            // must not prompt the user and poll on a response the server never signalled success for
            MockOidcServer.Handler handler = (method, path, body) ->
                    MockOidcServer.json(403, deviceAuthorizationJson(1, 300));
            AtomicBoolean prompted = new AtomicBoolean(false);
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, challenge -> prompted.set(true))) {
                try {
                    auth.getToken();
                    Assert.fail("expected the non-2xx device authorization response to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("unexpected response from the device authorization endpoint"));
                }
                Assert.assertFalse("the user must not be prompted on a rejected device authorization response", prompted.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNullAccessTokenNotServedAsLiteralNull() throws Exception {
        assertMemoryLeak(() -> {
            // a JSON null arrives from the lexer as the literal "null"; "access_token": null must be treated
            // as absent, not stored and served as the 4-char token "null"
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, "{\"token_type\":\"Bearer\",\"expires_in\":3600,\"access_token\":null}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    String token = auth.getToken();
                    Assert.fail("a JSON null access_token must not be served as the literal token \"null\" [got=" + token + "]");
                } catch (OidcAuthException e) {
                    // null is absent, so a 2xx with no token is a definitive but malformed answer
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("unexpected response"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNullJsonErrorIsTreatedAsAbsent() throws Exception {
        assertMemoryLeak(() -> {
            // "error": null in a device-auth response must be treated as absent, not as an OAuth error whose
            // code is the literal string "null"; the flow must proceed to prompt and poll
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"WDJB\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"error\":null,"
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-OK", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-OK", auth.getToken());
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
    public void testOauthErrorMessageStripsBidiControls() throws Exception {
        assertMemoryLeak(() -> {
            // an IdP error_description carrying a right-to-left override and a zero-width space (as JSON
            // unicode escapes the lexer decodes) must not reach the exception message verbatim; they would
            // let a malicious IdP reorder or hide text when the message is rendered to a terminal or a log
            String desc = "denied" + jsonUnicodeEscape(0x202E) + "reversed" + jsonUnicodeEscape(0x200B) + "end";
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
                    assertNoUnsafeDisplayChars(msg);
                    Assert.assertTrue(msg, msg.contains("access_denied"));
                    Assert.assertTrue(msg, msg.contains("deniedreversedend")); // readable text survives, controls gone
                }
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
    public void testOversizedSettingsBodyAbortsAtSizeCap() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or MITM'd server streams a /settings body larger than the client's response-size cap
            // (MAX_RESPONSE_BODY_BYTES, 4 MiB); the bounded read must abort on the cap rather than consume the
            // body without limit. Stream well past the cap - the client stops reading and closes the
            // connection once it crosses 4 MiB
            MockOidcServer.Handler handler = (method, path, body) -> MockOidcServer.oversizedJson(8L * 1024 * 1024);
            try (MockOidcServer server = new MockOidcServer(handler)) {
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), true);
                    Assert.fail("expected discovery to abort on the response-size cap");
                } catch (OidcAuthException e) {
                    // the size-cap failure surfaces as the cause; the body (which carries access/id/refresh
                    // tokens on a real response) is never embedded in the message
                    Throwable cause = e.getCause();
                    Assert.assertNotNull("expected the size-cap failure as the cause", cause);
                    Assert.assertTrue(cause.getMessage(), cause.getMessage().contains("exceeded the size limit"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPersistentTransportFailureDuringPollingAborts() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint works, but the (co-located) token endpoint drops the connection on every
            // poll; polling must abort with the underlying transport error after a few attempts, not retry
            // silently until the code expires. The endpoints share one origin so the build-time co-location
            // check passes - the mock simulates the unreachable token endpoint by dropping the connection
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 10));
                }
                return MockOidcServer.dropConnection();
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                        .clientId("questdb")
                        .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                        .tokenEndpoint(server.httpUrl(TOKEN_PATH))
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
    public void testRefreshTokenAlongsideErrorFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // a refresh response that carries an OAuth error (under a non-2xx status) must not be trusted
            // even if it also returns a token; the client ignores the smuggled token and falls back to a
            // fresh interactive sign-in rather than caching it
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // malformed: a 400 error together with a token
                    return MockOidcServer.json(400, "{\"error\":\"invalid_grant\",\"access_token\":\"SHOULD-NOT-BE-USED\"}");
                }
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.getToken());
                // the cached token is expired vs the skew; the refresh carries an error+token, so the
                // client must ignore the smuggled token and re-run the interactive flow
                Assert.assertEquals("ACCESS-2", auth.getToken());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
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
    public void testTokenAlongsideOauthErrorIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // RFC 6749 5.2: an error response must not be treated as a grant even if the body also carries
            // a token. A hostile or buggy IdP returns access_denied together with an access_token; the
            // client must surface the error, not cache the smuggled token
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"error\":\"access_denied\",\"access_token\":\"SHOULD-NOT-BE-USED\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected the error response to be rejected, not the smuggled token accepted");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("access_denied", e.getOauthError());
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("SHOULD-NOT-BE-USED"));
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
    public void testTokenResponseExpiresInIsClamped() throws Exception {
        assertMemoryLeak(() -> {
            // an absurd token-response expires_in (here Integer.MAX_VALUE, ~68 years) must be clamped like
            // the device-side value, so the client does not trust a stale cached token for decades. With the
            // clock-skew margin set above the clamp, a clamped token reads as already-expired on the next
            // call and getToken() re-runs the flow; an unclamped ~68-year cache would be served instead, so
            // the device endpoint would be hit only once.
            AtomicInteger deviceCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                // no refresh_token, so an expired cache forces a fresh device flow rather than a silent refresh
                return MockOidcServer.json(200, tokenJson("ACCESS-OK", null, null, Integer.MAX_VALUE));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .scope("openid")
                         .prompt(noopPrompt())
                         .allowInsecureTransport(true)
                         .clockSkewSeconds(7200) // 2h, above the 1h (MAX_EXPIRES_IN_SECONDS) clamp
                         .build()) {
                Assert.assertEquals("ACCESS-OK", auth.getToken());
                Assert.assertEquals("first sign-in runs the device flow once", 1, deviceCalls.get());
                // the clamped 1h TTL minus the 2h skew is already in the past, so the next call re-runs the
                // flow; without the clamp the ~68-year cache would be served and the flow would not run again
                Assert.assertEquals("ACCESS-OK", auth.getToken());
                Assert.assertEquals("clamped token expiry forces a fresh sign-in", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenUnderNonSuccessStatusIsNotAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // RFC 6749 5.1: a token must come from a 2xx response. A token under a non-2xx status with no
            // OAuth error is a malformed or hostile answer; the client must not cache it - it charges the
            // response to the transport-error budget and aborts rather than trusting the token
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"access_token\":\"SHOULD-NOT-BE-USED\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.getToken();
                    Assert.fail("expected a token under a 400 to be rejected, not accepted");
                } catch (OidcAuthException e) {
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("SHOULD-NOT-BE-USED"));
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("repeated unexpected responses"));
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

    private static void assertNoUnsafeDisplayChars(String value) {
        // mirrors OidcAuthException.isUnsafeForDisplay: no controls, no Cf format chars, no bidi/BOM -
        // checked per code point so a supplementary-plane (>= U+10000) format/control char is not missed
        for (int i = 0; i < value.length(); ) {
            int cp = value.codePointAt(i);
            boolean unsafe = Character.isISOControl(cp)
                    || Character.getType(cp) == Character.FORMAT
                    || Character.getType(cp) == Character.SURROGATE
                    || (cp >= 0x202A && cp <= 0x202E)
                    || (cp >= 0x2066 && cp <= 0x2069)
                    || cp == 0x200E || cp == 0x200F
                    || cp == 0xFEFF;
            Assert.assertFalse("display-unsafe char U+" + Integer.toHexString(cp) + " at index " + i + " in '" + value + "'", unsafe);
            i += Character.charCount(cp);
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

    // isLoopbackHost is a private static security classifier (it gates the plaintext-channel MITM pin); the
    // client is an open module, so reflection reaches it without widening production visibility for the test
    private static boolean invokeIsLoopbackHost(String host) throws Exception {
        Method m = OidcDeviceAuth.class.getDeclaredMethod("isLoopbackHost", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, host);
    }

    // builds a JSON unicode escape (backslash-u-XXXX) for a BMP code point without writing one literally
    // in this source (char 92 is REVERSE SOLIDUS), so the file stays ASCII; the client's JSON lexer decodes
    // the escape back into the real character, exercising the same decode-then-display path a hostile IdP hits
    private static String jsonUnicodeEscape(int codePoint) {
        String hex = Integer.toHexString(codePoint);
        return ((char) 92) + "u" + "0000".substring(hex.length()) + hex;
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

    private static String wellKnownJson(String deviceEndpoint, String tokenEndpoint, String issuer) {
        return "{"
                + "\"issuer\":\"" + issuer + "\","
                + "\"authorization_endpoint\":\"" + issuer + "/authorize\","
                + "\"token_endpoint\":\"" + tokenEndpoint + "\","
                + "\"device_authorization_endpoint\":\"" + deviceEndpoint + "\""
                + "}";
    }
}
