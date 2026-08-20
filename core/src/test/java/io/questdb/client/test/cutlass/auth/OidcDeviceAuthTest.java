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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.test.tools.NoBrowserLaunch;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class OidcDeviceAuthTest {

    /**
     * Every flow here that reaches the device-code prompt would otherwise pop a real browser tab on a
     * developer machine. A class rule rather than a static initializer, so the override is undone
     * afterwards instead of leaking into every later class in the surefire JVM.
     */
    @ClassRule
    public static final NoBrowserLaunch NO_BROWSER = new NoBrowserLaunch();

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
                OidcAuthException e = assertOidcFails(auth::signIn, "the user declined");
                Assert.assertEquals("access_denied", e.getOauthError());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAllControlVerificationUriCompleteTreatedAsAbsent() throws Exception {
        assertMemoryLeak(() -> {
            // a verification_uri_complete that is all control chars is non-empty on the wire but sanitizes to
            // empty; it must be treated as absent (null), so the prompt shows no blank "(or open this URL ...)"
            // line and the browser launcher is never handed an empty string
            String allControl = jsonUnicodeEscape(0x0001) + jsonUnicodeEscape(0x0002) + jsonUnicodeEscape(0x0003);
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"verification_uri_complete\":\"" + allControl + "\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-OK", null, null, 3600));
            };
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-OK", auth.signIn());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                Assert.assertNull(challenge.getVerificationUriComplete());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAllControlVerificationUriRejectedAsIncomplete() throws Exception {
        assertMemoryLeak(() -> {
            // a verification_uri made entirely of control chars is non-empty on the wire but sanitizes to empty
            // - it would display as a blank URL the user cannot open, so the response is rejected as incomplete
            // (the valid token below would let an unfixed client proceed to a successful but unusable sign-in)
            String allControl = jsonUnicodeEscape(0x0001) + jsonUnicodeEscape(0x0002);
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"" + allControl + "\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-OK", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                assertOidcFails(auth::signIn, "incomplete",
                        "expected an all-control verification_uri to be rejected as incomplete");
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
                Assert.assertEquals("ACCESS-AUD", auth.signIn());
                Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("audience=api%3A%2F%2Fquestdb"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testAudienceSentOnRefresh() throws Exception {
        assertMemoryLeak(() -> {
            // the audience must also be url-encoded into the refresh request, matching the Python client
            AtomicReference<String> refreshBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshBody.set(body);
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 60));
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth); // force the silent-refresh path on the next call
                Assert.assertEquals("ACCESS-2", auth.signIn());
                Assert.assertTrue(refreshBody.get(), refreshBody.get().contains("audience=api%3A%2F%2Fquestdb"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderIssuerPinAcceptsHostCasingAndImplicitPort() throws Exception {
        assertMemoryLeak(() -> {
            // the origin pin (isSameOrigin) folds host case (ASCII) and treats an implicit https port as 443, so
            // an endpoint differing from the issuer only in host case or an explicit :443 is still same-origin
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://IDP.Example:443/as/device")
                    .tokenEndpoint("https://idp.example/as/token")
                    .issuer("https://Idp.Example")
                    .build()
            ) {
                // accepted: host-case and implicit-vs-explicit 443 differences do not defeat the origin pin
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderIssuerPinAcceptsMatchingOrigin() throws Exception {
        assertMemoryLeak(() -> {
            // endpoints that belong to the pinned issuer origin are accepted; only the origin is pinned, so
            // the differing paths of the device and token endpoints are fine
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://idp.example/as/device")
                    .tokenEndpoint("https://idp.example/as/token")
                    .issuer("https://idp.example")
                    .build()
            ) {
                // accepted: build() did not reject the matching-origin endpoints
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBuilderIssuerPinRejectsOffOriginEndpoints() {
        // the token/device endpoints do not belong to the pinned issuer origin; build() must reject them
        // rather than send the device code and refresh token outside the trusted issuer
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                .clientId("c")
                .deviceAuthorizationEndpoint("https://idp.example/device")
                .tokenEndpoint("https://idp.example/token")
                .issuer("https://other-idp.example")
                .build()
        ) {
            Assert.fail("expected the issuer pin to reject off-origin endpoints");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not match the issuer origin"));
        }
    }

    @Test(timeout = 30_000)
    public void testBuilderRejectsMissingRequiredOptions() {
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder().deviceAuthorizationEndpoint("https://h/d").tokenEndpoint("https://h/t").build()) {
            Assert.fail("expected clientId validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("clientId"));
        }
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder().clientId("c").tokenEndpoint("https://h/t").build()) {
            Assert.fail("expected deviceAuthorizationEndpoint validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("deviceAuthorizationEndpoint"));
        }
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder().clientId("c").deviceAuthorizationEndpoint("https://h/d").build()) {
            Assert.fail("expected tokenEndpoint validation to fail");
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("tokenEndpoint"));
        }
    }

    @Test(timeout = 30_000)
    public void testBuilderRejectsNonPositiveHttpTimeout() {
        // every other timing input is clamped; a non-positive HTTP timeout yields an already-expired read
        // deadline and an unbounded recv(int), so the setter rejects it (matching Sender.Builder)
        for (int bad : new int[]{0, -1}) {
            try {
                OidcDeviceAuth.builder().httpTimeoutMillis(bad);
                Assert.fail("expected httpTimeoutMillis(" + bad + ") to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("httpTimeoutMillis"));
            }
        }
    }

    @Test(timeout = 30_000)
    public void testBuilderRejectsSplitOriginEndpoints() {
        // the token and device authorization endpoints are on different origins; RFC 8628 co-locates them
        // on one authorization server, so build() must refuse to spread the credential POSTs across hosts
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                .clientId("c")
                .deviceAuthorizationEndpoint("https://device.example/device")
                .tokenEndpoint("https://token.example/token")
                .build()
        ) {
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
                Assert.assertEquals("ACCESS-OK", auth.signIn());
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
                Assert.assertEquals("ACCESS-OK", auth.signIn());
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
                Assert.assertEquals("ACCESS-OK", auth.signIn());
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
                Assert.assertEquals("ACCESS-OK", auth.signIn());
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
            String idToken = TestUtils.repeat("a", 3000);
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.chunkedJson(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.chunkedJson(200, tokenJson("ACCESS-CHUNKED", idToken, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                // groups-in-token mode serves the id token; it arrived chunked and is 3 KB long
                Assert.assertEquals(idToken, auth.signIn());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClearCacheForcesFreshSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // clearCache() must drop the cached token AND the refresh token, so the next signIn() runs a
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                auth.clearCache();
                // the next call must run a second device-code sign-in, not a refresh (the refresh token was dropped)
                Assert.assertEquals("ACCESS-1", auth.signIn());
                Assert.assertEquals("clearCache must force a second interactive sign-in", 2, deviceCalls.get());
                Assert.assertEquals("clearCache must drop the refresh token so no refresh is attempted", 0, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testClockSkewCappedAtHalfTokenLifetime() throws Exception {
        assertMemoryLeak(() -> {
            // the fixed 30s clock skew is capped at half the token lifetime (matching the Python client), so a
            // short-lived token is served from cache for the first half of its life rather than being treated
            // as expired the instant it is issued - which a flat 30s skew would do to any sub-60s token
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshCalls.incrementAndGet();
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 10)); // 10s lifetime
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .allowInsecureTransport(true)
                         .prompt(noopPrompt())
                         .build()) {
                // a flat 30s skew would mark this 10s token expired immediately (now < expiresAt - 30s is
                // false); the lifetime/2 cap (5s) keeps it valid, so the second call is a cache hit, not a refresh
                Assert.assertEquals("ACCESS-1", auth.signIn());
                Assert.assertEquals("ACCESS-1", auth.signIn());
                Assert.assertEquals("the capped skew kept the short token cached - no refresh", 0, refreshCalls.get());

                // once the token is genuinely past expiry, signIn() takes the silent-refresh path
                expireCachedToken(auth);
                Assert.assertEquals("ACCESS-2", auth.signIn());
                Assert.assertEquals(1, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseCancelsInFlightSignIn() throws Exception {
        // a sign-in is waiting for the user: the token endpoint keeps returning authorization_pending.
        // close() from another caller must abort the in-flight signIn() promptly, instead of letting
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
                        auth.signIn();
                        outcome.set(new AssertionError("signIn() should have been cancelled by close()"));
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
                Assert.assertFalse("signIn() did not return promptly after close()", signIn.isAlive());
                Throwable t = outcome.get();
                Assert.assertTrue("expected an OidcAuthException, got " + t, t instanceof OidcAuthException);
                Assert.assertTrue(t.getMessage(), t.getMessage().contains("closed"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConcurrentSignInStartsSingleSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // several callers race signIn() on a fresh instance; the synchronized method must serialize
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
                            tokens[idx] = auth.signIn();
                        } catch (Throwable t) {
                            error.set(t);
                        }
                    }, "oidc-signIn-" + i);
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
    public void testDeviceCodeLifetimeClamped() throws Exception {
        assertMemoryLeak(() -> {
            // a missing or zero expires_in defaults to 600s, and an absurd value is capped at 1800s (matching
            // the Python client), so a hostile or buggy provider cannot make the client poll for an absurd
            // duration; the clamped value is the one shown to the user (challenge.getExpiresInSeconds())
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            MockOidcServer.Handler missingExpiry = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"device_code\":\"DEV\",\"user_code\":\"UC\","
                            + "\"verification_uri\":\"https://verify.example/device\",\"interval\":1}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-DEFAULT-TTL", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(missingExpiry);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-DEFAULT-TTL", auth.signIn());
                Assert.assertEquals(600, shown.get().getExpiresInSeconds());
            }
            MockOidcServer.Handler absurdExpiry = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 999_999));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-CAPPED-TTL", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(absurdExpiry);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                Assert.assertEquals("ACCESS-CAPPED-TTL", auth.signIn());
                Assert.assertEquals(1800, shown.get().getExpiresInSeconds());
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
                OidcAuthException e = assertOidcFails(auth::signIn, "unknown client");
                Assert.assertEquals("invalid_client", e.getOauthError());
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
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
    public void testMalformedResponseHeadDuringDiscoveryIsAnOidcAuthException() throws Exception {
        assertMemoryLeak(() -> {
            // HttpHeaderParser rejects a response head it cannot parse - here a header block past its fixed
            // 4096-byte buffer, the shape a WAF or proxy stacking Set-Cookie/CSP produces - by throwing
            // HttpException. That is a SIBLING of HttpClientException, not a subclass, so it escaped both of
            // fetchJson's catches and left fromQuestDB throwing a type its own javadoc does not name, past
            // every caller's catch (OidcAuthException) degrade handler.
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (SETTINGS_PATH.equals(path)) {
                    StringBuilder padding = new StringBuilder();
                    for (int i = 0; i < 5000; i++) {
                        padding.append('A');
                    }
                    return MockOidcServer.raw("HTTP/1.1 200 OK\r\n"
                            + "X-Pad: " + padding + "\r\n"
                            + "Content-Length: 0\r\n\r\n");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                try {
                    OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure()).close();
                    Assert.fail("an unparseable response head must not gate discovery open");
                } catch (OidcAuthException expected) {
                    // the documented type; an HttpException escaping here is the regression
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testMalformedResponseHeadDuringPollingIsTransient() throws Exception {
        assertMemoryLeak(() -> {
            // The same unparseable head on the TOKEN endpoint, mid-poll. Escaping as HttpException it missed
            // postForm's catch and the client.disconnect() with it, so the cached keep-alive connection kept a
            // half-read response for the next poll to parse as its own; it also missed pollForToken's
            // classification, aborting the whole interactive sign-in on a condition the same loop rides out
            // when it arrives as a transport error. One malformed answer must not end a sign-in.
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (tokenCalls.incrementAndGet() == 1) {
                    StringBuilder padding = new StringBuilder();
                    for (int i = 0; i < 5000; i++) {
                        padding.append('A');
                    }
                    return MockOidcServer.raw("HTTP/1.1 200 OK\r\n"
                            + "X-Pad: " + padding + "\r\n"
                            + "Content-Length: 0\r\n\r\n");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-AFTER-RECOVERY", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-AFTER-RECOVERY", auth.signIn());
                Assert.assertTrue("the poll must have retried after the malformed head, on a clean connection",
                        tokenCalls.get() >= 2);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNonNumericStatusCodeRejected() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or MITM'd identity provider returns a status line whose status-code token carries an
            // ANSI escape (the HTTP header parser copies the token verbatim apart from SP/CR/LF). A status code
            // is bare digits, so a non-digit byte is a malformed or hostile status line: the client must reject
            // it - never echoing its bytes (which could rewrite a terminal or forge a log line) and never
            // trusting its leading digit as a 2xx success gate
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"config\":{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                            + "}}");
                }
                // status code "2<ESC>[m00": an ANSI reset spliced into the token. The leading '2' would pass a
                // first-char success check, but the non-digit bytes must make the client reject the response
                return MockOidcServer.raw("HTTP/1.1 2\u001b[m00 OK\r\n"
                        + "Content-Type: application/json\r\n"
                        + "Content-Length: 2\r\n"
                        + "\r\n"
                        + "{}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    auth.signIn();
                    Assert.fail("expected a malformed status code to be rejected");
                } catch (OidcAuthException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(msg, msg.contains("malformed HTTP status code"));
                    Assert.assertFalse("raw ESC must not leak into the message: " + msg, msg.indexOf('\u001b') >= 0);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNonNumericStatusCodeRejectedDuringPolling() throws Exception {
        assertMemoryLeak(() -> {
            // the malformed-status guard must also fire on the token-poll path, where readResponse handles the
            // POSTs that carry the device code on every poll (testNonNumericStatusCodeRejected covers the
            // device-authorization POST). The device step succeeds, then the token endpoint returns a status
            // line whose status-code token splices in an ANSI escape; the client must reject it - never echoing
            // its bytes, never trusting its leading '2' as a 2xx success gate
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.raw("HTTP/1.1 2\u001b[m00 OK\r\n"
                        + "Content-Type: application/json\r\n"
                        + "Content-Length: 2\r\n"
                        + "\r\n"
                        + "{}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "malformed HTTP status code",
                        "expected a malformed status code on the poll path to be rejected");
                String msg = e.getMessage();
                Assert.assertFalse("raw ESC must not leak into the message: " + msg, msg.indexOf('\u001b') >= 0);
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
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    Assert.assertEquals("ACCESS-SCOPE", auth.signIn());
                    Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("scope=openid"));
                    Assert.assertFalse(deviceBody.get(), deviceBody.get().contains("groups"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryRejectsMalformedStatusWithoutEchoingIt() throws Exception {
        assertMemoryLeak(() -> {
            // The header parser copies the status-line token verbatim apart from SP/CR/LF, so a non-digit
            // byte means a malformed or hostile status line. It must not be read as a 2xx by its leading
            // digit, and none of it may reach the exception message, which lands in logs and terminals.
            // A short all-digit status is malformed too, for the same "leading digit is not the class"
            // reason.
            // A COMPLETE, otherwise-valid settings body, so the only thing standing between this response
            // and a working instance is the status gate. A partial body would fail later on a missing key
            // and prove nothing about the status.
            for (String statusToken : new String[]{"2\u001b[31m0", "2"}) {
                AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
                MockOidcServer.Handler handler = (method, path, requestBody) -> {
                    MockOidcServer server = serverRef.get();
                    if (SETTINGS_PATH.equals(path)) {
                        String settings = settingsJson(true, true,
                                server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH));
                        return MockOidcServer.raw(
                                "HTTP/1.1 " + statusToken + " OK\r\n"
                                        + "Content-Type: application/json\r\n"
                                        + "Content-Length: " + settings.length() + "\r\n\r\n"
                                        + settings);
                    }
                    return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
                };
                try (MockOidcServer server = new MockOidcServer(handler)) {
                    serverRef.set(server);
                    OidcAuthException e = assertOidcFails(
                            () -> OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure()),
                            "malformed HTTP status code",
                            "a malformed status [" + statusToken + "] must not gate discovery open");
                    Assert.assertFalse("the raw status must not be echoed: " + e.getMessage(),
                            e.getMessage().indexOf('\u001b') >= 0);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testSettingsUnderErrorStatusNotTrustedAsConfig() throws Exception {
        assertMemoryLeak(() -> {
            // /settings was parsed without looking at the status, so a body carrying the right keys was
            // read as configuration whatever the response claimed to be. A 500 is not a settings document:
            // an error envelope, a proxy's branded page or a captive portal could supply the endpoints the
            // user then signs in against, and the refresh token is POSTed to. The status gate must refuse
            // it before the body is parsed at all.
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(500,
                            settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                OidcAuthException e = assertOidcFails(
                        () -> OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure()),
                        "did not return its settings",
                        "a 500 /settings body must not be trusted as OIDC configuration");
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("httpStatus=500"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testWellKnownUnderErrorStatusNotTrustedAsDiscoveryDoc() throws Exception {
        assertMemoryLeak(() -> {
            // The same hole on the .well-known fallback, which is what a pinned issuer falls back to when
            // /settings advertises no device endpoint. A 404 body is not a discovery document - a
            // tenant-not-found stub is exactly the shape that reaches this path - so it must not be able to
            // name the token and device endpoints.
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, settingsJson(true, false, server.httpUrl(TOKEN_PATH), null));
                }
                if (WELL_KNOWN_PATH.equals(path)) {
                    return MockOidcServer.json(404,
                            wellKnownJson(server.httpUrl(DEVICE_PATH), server.httpUrl(TOKEN_PATH), server.httpUrl("")));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(
                        server.httpUrl(""), insecure().issuer(server.httpUrl("")))) {
                    Assert.fail("a 404 .well-known body must not be trusted as a discovery document");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(),
                            e.getMessage().contains("did not return an OIDC discovery document"));
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("httpStatus=404"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryIgnoresArrayWrappedConfig() throws Exception {
        assertMemoryLeak(() -> {
            // a tampered /settings wraps the config object in an ARRAY - {"config":[{...}]} - so the config
            // keys sit inside an array element rather than the trusted top-level "config" object. The parser
            // must not surface an array element's object as config (mirroring FileTokenStore's array
            // rejection), so OIDC reads as disabled and fromQuestDB fails rather than trusting wrapped config.
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"config\":[{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.scope\":\"openid\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                            + "}]}");
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-X", "ID-X", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                assertOidcFails(() -> OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure()),
                        "OIDC is not enabled", "array-wrapped config must not be trusted as OIDC config");
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
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    // enabled stayed true (no DoS), groups-in-token stayed false (access token served),
                    // scope stayed "openid" (no injection)
                    Assert.assertEquals("ACCESS-TRUSTED", auth.signIn());
                    Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("scope=openid"));
                    Assert.assertFalse(deviceBody.get(), deviceBody.get().contains("INJECTED"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryReadsAudience() throws Exception {
        assertMemoryLeak(() -> {
            // the audience advertised by /settings (acl.oidc.audience) must be url-encoded into the device
            // authorization request, matching the Python client
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            AtomicReference<String> deviceBody = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"config\":{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.audience\":\"api://questdb\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl(TOKEN_PATH) + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(DEVICE_PATH) + "\""
                            + "}}");
                }
                if (DEVICE_PATH.equals(path)) {
                    deviceBody.set(body);
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-AUD-D", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    Assert.assertEquals("ACCESS-AUD-D", auth.signIn());
                    Assert.assertTrue(deviceBody.get(), deviceBody.get().contains("audience=api%3A%2F%2Fquestdb"));
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    Assert.fail("expected discovery to fail");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("token endpoint"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseWipesCredentialState() throws Exception {
        assertMemoryLeak(() -> {
            // close() disables every token operation, so nothing it holds can be needed again - yet the
            // instance went on holding all of it: the served token and refresh token in their String fields,
            // and the raw grant in the sinks that carried it. formSink keeps the last request body, which on
            // the refresh path is literally "refresh_token=<the token>"; the two response parsers keep every
            // field of the last response, device code included. All are reused, and clear() only rewinds the
            // write position, so a long secret followed by a short write stays legible in the tail.
            //
            // The walk below is deliberately reflective and generic rather than a list of field names: a sink
            // added to this class or to either parser later is covered without anyone remembering to extend
            // the test.
            AtomicInteger deviceCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEVCODE-WIPE-ME\","
                            + "\"user_code\":\"USERCODE-WIPE-ME\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"expires_in\":300,\"interval\":1}");
                }
                if (body.contains("grant_type=refresh_token")) {
                    return MockOidcServer.json(200,
                            tokenJson("ACCESS-REFRESHED-WIPE-ME", "ID-REFRESHED-WIPE-ME", "REFRESH-2-WIPE-ME", 3600));
                }
                return MockOidcServer.json(200,
                        tokenJson("ACCESS-WIPE-ME", "ID-WIPE-ME", "REFRESH-1-WIPE-ME", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                OidcDeviceAuth auth = newAuth(server, false, noopPrompt());
                try {
                    Assert.assertEquals("ACCESS-WIPE-ME", auth.signIn());
                    expireCachedToken(auth);
                    // spend the refresh token too, so it passes through formSink as a request parameter
                    Assert.assertEquals("ACCESS-REFRESHED-WIPE-ME", auth.getToken());
                    Assert.assertEquals(1, deviceCalls.get());
                    // the state is genuinely there before the close - otherwise the sweep below proves nothing
                    assertHoldsSomewhere(auth, "REFRESH-2-WIPE-ME");
                } finally {
                    auth.close();
                }

                Assert.assertNull("the served access token must not survive close()",
                        readField(auth, "accessToken"));
                Assert.assertNull("the id token must not survive close()", readField(auth, "idToken"));
                Assert.assertNull("the refresh token must not survive close()", readField(auth, "refreshToken"));
                Assert.assertNull("the last-persisted refresh token must not survive close()",
                        readField(auth, "lastPersistedRefreshToken"));
                for (String secret : new String[]{
                        "ACCESS-WIPE-ME", "ID-WIPE-ME", "REFRESH-1-WIPE-ME",
                        "ACCESS-REFRESHED-WIPE-ME", "ID-REFRESHED-WIPE-ME", "REFRESH-2-WIPE-ME",
                        "DEVCODE-WIPE-ME", "USERCODE-WIPE-ME"}) {
                    assertHoldsNowhere(auth, secret);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDiscoveryTransportFailureDoesNotLeakNativeMemory() throws Exception {
        // discoverSettings allocates a JSON lexer (NATIVE_TEXT_PARSER_RSS) and an HTTP client (NATIVE_DEFAULT
        // buffers) and frees both in a finally; a transport failure during discovery must not leak either.
        // assertMemoryLeak covers EVERY tag - its LeakCheck asserts per-tag equality across the whole
        // MemoryTag range, then total equality - so it is the outer guard here rather than something to work
        // around. The two explicit tag assertions stay because they name the buffer that leaked, which a
        // blanket "total native memory" mismatch does not.
        assertMemoryLeak(() -> {
            int deadPort;
            try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                deadPort = probe.getLocalPort();
            } // closed now - nothing listens on deadPort
            long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
            long clientMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB("http://127.0.0.1:" + deadPort, insecure())) {
                Assert.fail("expected discovery to fail against a dead port");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not reach the QuestDB server"));
            }
            Assert.assertEquals("the discovery JSON lexer native buffer leaked",
                    parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
            Assert.assertEquals("the discovery HTTP client native buffers leaked",
                    clientMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
        });
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
                Assert.assertEquals("ACCESS-LAST", auth.signIn());
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
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://idp.example/dev" + marker + "ice")
                    .tokenEndpoint("https://idp.example/t")
                    .build()
            ) {
                Assert.fail("expected the display-unsafe url to be rejected [index=" + i + "]");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("illegal character"));
                // the raw unsafe character must not survive into the message
                assertNoUnsafeDisplayChars(e.getMessage());
            }
        }
    }

    @Test(timeout = 30_000)
    public void testEndpointParseAcceptsUppercaseScheme() throws Exception {
        assertMemoryLeak(() -> {
            // RFC 3986 schemes are case-insensitive, so HTTPS/Http must build - matching BrowserLauncher's
            // case-insensitive scheme allowlist. (Endpoint.parse lower-cases only ASCII, so a homoglyph scheme
            // is still rejected as "expected http or https".)
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("HTTPS://idp.example/device")
                    .tokenEndpoint("Https://idp.example/token")
                    .build()
            ) {
                // accepted: build() did not reject the mixed-case https scheme
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
        // userinfo (user@host or user:pass@host) is unsupported: the HTTP layer would connect to the literal
        // "user@host", so reject it rather than mis-resolve it or report a misleading port-parse error
        assertBuildFails("https://user@idp/d", "https://idp/t", "userinfo");
        assertBuildFails("https://idp/d", "https://user:pass@idp/t", "userinfo");
        // an out-of-range port (0, negative, or above 65535) is rejected rather than passed to the transport
        assertBuildFails("https://idp:99999/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp:0/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp:-1/d", "https://idp/t", "between 1 and 65535");
        assertBuildFails("https://idp/d", "https://idp:70000/t", "between 1 and 65535");
        // a leading '+' on the port is rejected: Integer.parseInt would read ":+443" as 443, but a real
        // authority port is bare digits (a leading '-' is already caught by the range check above)
        assertBuildFails("https://idp:+443/d", "https://idp/t", "could not parse the port");
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
        // a fragment (#...) is rejected: pathOnly() strips it before the issuer-path pin while postForm sends
        // endpoint.path verbatim on the wire, so folding a "#/../other/token" past the '#' would let a lenient
        // server that normalizes '..' resolve the request-target to a path the pin never validated. Fail closed
        assertBuildFails("https://idp/realms/acme#/../other/device", "https://idp/realms/acme/token", "fragment");
        assertBuildFails("https://idp/d", "https://idp/realms/acme#/../other/token", "fragment");
        assertBuildFails("https://idp/d#", "https://idp/t", "fragment");
        // a query (?...) is rejected for the same pin-bypass reason: pathOnly() strips it before the issuer-path
        // pin while postForm sends endpoint.path - query included - verbatim, so a "?..." the pin never validated
        // would still reach the wire. An OIDC device/token endpoint carries its parameters in the request body,
        // never the url query, so fail closed (the user-facing verification url may carry one, but it is parsed
        // by BrowserLauncher, not Endpoint.parse)
        assertBuildFails("https://idp/realms/acme/device?x=/../other", "https://idp/realms/acme/token", "query");
        assertBuildFails("https://idp/d", "https://idp/realms/acme/token?client_id=evil", "query");
        assertBuildFails("https://idp/d?a=b", "https://idp/t", "query");
        // a non-ASCII host is rejected: it would not resolve (the HTTP layer sends the host to the OS resolver
        // as raw UTF-8, no IDNA), and equalsIgnoreCase folds several non-ASCII letters onto ASCII (U+0130 -> i,
        // U+212A -> k, ...), so a homoglyph host could otherwise pass the origin pin against a pinned issuer
        assertBuildFails("https://\u0130dp/d", "https://idp/t", "non-ASCII"); // U+0130, folds to i
        assertBuildFails("https://idp/d", "https://\u212Aelvin/t", "non-ASCII"); // U+212A Kelvin, folds to k
        // a backslash in the host is rejected: the WHATWG URL spec folds '\' to '/', so a lenient consumer
        // could re-split good.com\.evil.com into a different authority
        assertBuildFails("https://good.com\\.evil.com/d", "https://idp/t", "backslash");
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
                Assert.assertEquals("ACCESS-DC", auth.signIn());
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
                OidcAuthException e = assertOidcFails(auth::signIn, "it\"s a / test");
                Assert.assertEquals("access_denied", e.getOauthError());
                // the escapes are decoded, not shown literally
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("\\/"));
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
                Assert.assertEquals("ACCESS-ESC", auth.signIn());
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
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("")))) {
                    // settings advertise groups.encoded.in.token=true, so signIn() returns the id token
                    Assert.assertEquals("ID-WK", auth.signIn());
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("")))) {
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
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
                    // discovery advertises groups.encoded.in.token=true, so signIn() must return the id token
                    Assert.assertEquals("ID-D", auth.signIn());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFromQuestDbIssuerPinAcceptsOffOriginDiscoveredEndpoints() throws Exception {
        assertMemoryLeak(() -> {
            // The Google case: the pinned issuer hosts its discovery document on one origin but serves its
            // token and device endpoints on another. /settings advertises neither endpoint, so both are
            // discovered from the issuer's own .well-known (a trusted, out-of-band source) and must be accepted
            // wherever the issuer hosts them, NOT origin-pinned to the issuer. An endpoint the untrusted
            // /settings advertised IS still origin-pinned - see testFromQuestDbIssuerPinRejectsOffOriginAdvertisedEndpoint.
            AtomicReference<MockOidcServer> idpRef = new AtomicReference<>();
            AtomicReference<MockOidcServer> issuerRef = new AtomicReference<>();
            // the IdP endpoint host: a different origin (port) than the issuer below
            MockOidcServer.Handler idpHandler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-OFF", null, null, 3600));
            };
            try (MockOidcServer idp = new MockOidcServer(idpHandler)) {
                idpRef.set(idp);
                // the QuestDB server doubles as the pinned issuer: it serves /settings (advertising neither
                // endpoint) and the .well-known document, which points the device/token endpoints at the
                // off-origin idp
                MockOidcServer.Handler issuerHandler = (method, path, body) -> {
                    MockOidcServer endpointHost = idpRef.get();
                    MockOidcServer iss = issuerRef.get();
                    if (SETTINGS_PATH.equals(path)) {
                        return MockOidcServer.json(200, "{\"config\":{"
                                + "\"acl.oidc.enabled\":true,"
                                + "\"acl.oidc.client.id\":\"questdb\","
                                + "\"acl.oidc.scope\":\"openid\""
                                + "}}");
                    }
                    if (WELL_KNOWN_PATH.equals(path)) {
                        return MockOidcServer.json(200, wellKnownJson(
                                endpointHost.httpUrl(DEVICE_PATH), endpointHost.httpUrl(TOKEN_PATH), iss.httpUrl("")));
                    }
                    return MockOidcServer.json(404, "{}");
                };
                try (MockOidcServer issuer = new MockOidcServer(issuerHandler)) {
                    issuerRef.set(issuer);
                    try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(issuer.httpUrl(""), insecure().issuer(issuer.httpUrl("")))) {
                        // the off-origin discovered endpoints are accepted; the device flow completes against them
                        Assert.assertEquals("ACCESS-OFF", auth.signIn());
                    }
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer("https://idp.attacker.example"))) {
                    Assert.fail("expected the issuer pin to reject the off-origin endpoints");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("is not on the pinned identity-provider origin"));
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
        try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB("http://questdb.example:9000")) {
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
            // failure out of signIn()
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the cached token is expired and the refresh body is garbled, so the client must re-run
                // the interactive flow instead of throwing the parse error
                Assert.assertEquals("ACCESS-2", auth.signIn());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenDoesNotBlockBehindInteractiveSignIn() throws Exception {
        assertMemoryLeak(() -> {
            // an interactive signIn() is parked polling (authorization_pending), holding the instance
            // lock for the whole device-code lifetime. A flush-path getToken() on another thread
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
                        auth.signIn();
                    } catch (Throwable ignore) {
                        // expected: cancelled by close() at the end of the test
                    }
                }, "oidc-sign-in");
                signIn.setDaemon(true);
                signIn.start();
                try {
                    // wait until the interactive flow has prompted and is polling (it holds the lock now)
                    Assert.assertTrue("the sign-in did not reach the polling stage", polling.await(10, TimeUnit.SECONDS));
                    // getToken() must return control promptly (here: throw), NOT block ~10s until
                    // the device code expires and signIn() releases the lock
                    long startNanos = System.nanoTime();
                    OidcAuthException e = assertOidcFails(auth::getToken, "in progress",
                            "expected getToken() to fail fast while a sign-in is in progress");
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    Assert.assertTrue("getToken() blocked " + elapsedMillis + "ms behind the in-flight sign-in",
                            elapsedMillis < 2_000);
                } finally {
                    auth.close();        // cancel the in-flight sign-in
                    signIn.join(10_000); // let the daemon thread unwind before the leak check
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenSucceedsWhenCallingThreadIsInterrupted() throws Exception {
        assertMemoryLeak(() -> {
            // getToken()'s uncontended lock acquire must NOT fail merely because the calling thread carries a
            // set interrupt flag. An ILP producer on a pooled/managed thread commonly does (interrupt is the
            // standard cancellation signal), and the old timed tryLock threw InterruptedException even on a FREE
            // lock and then re-armed the flag, so every getToken() on that thread failed with a valid token
            // sitting in the cache. The untimed fast-path acquire fixes it.
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.signIn()); // seed a valid cached token

                Thread.currentThread().interrupt(); // the calling (producer) thread carries a pending interrupt
                try {
                    // uncontended lock, valid cached token: getToken() must return it, not throw on the interrupt
                    Assert.assertEquals("ACCESS-1", auth.getToken());
                    // and it must not silently swallow the caller's interrupt (the untimed acquire preserves it)
                    Assert.assertTrue("getToken() must not clear the caller's interrupt flag",
                            Thread.currentThread().isInterrupted());
                } finally {
                    Thread.interrupted(); // clear so the flag does not leak into later tests sharing this fork
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenWaitsBehindSilentRefreshInsteadOfFailing() throws Exception {
        assertMemoryLeak(() -> {
            // When another thread's SILENT REFRESH (not an interactive sign-in) holds the lock, a second
            // getToken() must WAIT for that bounded refresh and then serve the freshly refreshed token - NOT
            // fail fast. Failing fast would make every concurrent caller sharing one OidcDeviceAuth (the
            // documented shared-provider pattern) spuriously throw on each token refresh. The token endpoint
            // blocks the refresh response until the test releases it, pinning the lock on the refresher thread
            // while the second caller waits for it. (This is the fix for the old fail-fast-on-any-contention
            // behaviour: the HttpTokenProvider contract permits a brief wait behind a silent refresh.)
            CountDownLatch refreshInFlight = new CountDownLatch(1);
            CountDownLatch releaseRefresh = new CountDownLatch(1);
            AtomicReference<String> handlerError = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshInFlight.countDown();
                    try {
                        if (!releaseRefresh.await(30, TimeUnit.SECONDS)) {
                            // on a MockOidcServer thread: JUnit would swallow an Assert.fail here, so record it
                            // and let the main thread assert on it at the end
                            handlerError.set("the test never released the refresh within 30s");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", null, "REFRESH-2", 3600));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1)); // initial device_code grant
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .allowInsecureTransport(true)
                         .prompt(noopPrompt())
                         .build()) {
                auth.signIn(); // sign in once: caches ACCESS-1 and a refresh token
                expireCachedToken(auth); // so the refresher thread's getToken() takes the refresh path
                // The refresher is not scenery: it is the thread that holds the lock, performs the refresh
                // and produces ACCESS-2. Swallowing its failure let the test pass on a run where the refresh
                // never happened - the waiter would simply refresh for itself and still see ACCESS-2, so
                // every assertion below still held while the contention this test exists for never occurred.
                AtomicReference<Throwable> refresherError = new AtomicReference<>();
                AtomicReference<String> refresherResult = new AtomicReference<>();
                Thread refresher = new Thread(() -> {
                    try {
                        refresherResult.set(auth.getToken());
                    } catch (Throwable t) {
                        refresherError.set(t);
                    }
                }, "oidc-silent-refresh");
                refresher.setDaemon(true);
                refresher.start();
                Assert.assertTrue("the silent refresh did not start", refreshInFlight.await(10, TimeUnit.SECONDS));

                // a refresh holds the lock now; a second getToken() must WAIT for it, not fail fast
                AtomicReference<String> waiterResult = new AtomicReference<>();
                AtomicReference<Throwable> waiterError = new AtomicReference<>();
                Thread waiter = new Thread(() -> {
                    try {
                        waiterResult.set(auth.getToken());
                    } catch (Throwable t) {
                        waiterError.set(t);
                    }
                }, "oidc-getToken-waiter");
                waiter.setDaemon(true);
                waiter.start();
                // Wait until the waiter is genuinely INSIDE getToken(), read off its own stack. The latch this
                // replaced counted down as the first statement of the thread body - BEFORE the call it claimed
                // to gate - so it proved only that the thread had been scheduled, and every "still blocked"
                // assertion below rested on the sleep that follows instead.
                Assert.assertTrue("the waiter never entered getToken()", awaitInside(waiter, "getToken", 10_000));
                try {
                    // give the waiter time to (wrongly) fail fast if it were going to; while the refresh is held
                    // it must instead still be blocked INSIDE getToken() - a fail-fast throw would have left that
                    // frame (and finished the thread)
                    Thread.sleep(500);
                    Assert.assertTrue("getToken() must still be blocked behind the peer's refresh, not finished",
                            waiter.isAlive());
                    Assert.assertTrue("getToken() must still be inside the call, waiting out the peer's refresh",
                            isInside(waiter, "getToken"));
                    Assert.assertNull("getToken() must not fail fast behind a silent refresh, but threw: " + waiterError.get(),
                            waiterError.get());
                    Assert.assertNull("getToken() must wait, not return, while the peer's refresh is still in flight",
                            waiterResult.get());
                } finally {
                    releaseRefresh.countDown();
                    waiter.join(10_000);
                    refresher.join(10_000);
                }
                // once the peer's refresh completed and released the lock, the waiter served the fresh token
                Assert.assertNull("the refresher itself failed, so the wait was never behind a real refresh: "
                        + refresherError.get(), refresherError.get());
                Assert.assertEquals("the refresher must have completed the refresh it was holding the lock for",
                        "ACCESS-2", refresherResult.get());
                Assert.assertNull("getToken() must not throw when it waits out a peer's refresh: " + waiterError.get(),
                        waiterError.get());
                Assert.assertEquals("getToken() must serve the freshly refreshed token after waiting", "ACCESS-2", waiterResult.get());
                Assert.assertNull("the mock server handler must not have reported an error", handlerError.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenRefreshesWhenServedKindIsNullButRefreshTokenExists() throws Exception {
        assertMemoryLeak(() -> {
            // groupsInToken=true, but the device-code grant returns an access_token + refresh_token and NO
            // id_token: signIn() rejects that grant (the served id_token is missing) yet leaves the refresh token
            // in memory. A later getToken() must then attempt a silent refresh - which here yields the id_token -
            // rather than give up with "no token has been obtained yet". M5: the refresh is no longer foreclosed
            // just because the served-kind token is currently null.
            AtomicInteger refreshCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    refreshCalls.incrementAndGet();
                    return MockOidcServer.json(200, tokenJson("ACCESS-2", "ID-2", "REFRESH-2", 3600)); // now with id_token
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 3600)); // initial grant: no id_token
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) { // groupsInToken=true
                assertOidcFails(auth::signIn, "no id_token",
                        "signIn() must reject a grant with no id_token when groups are encoded in the token");
                // the partial grant left a refresh token in memory; getToken() must refresh to obtain the id_token
                Assert.assertEquals("ID-2", auth.getToken());
                Assert.assertEquals("getToken() must have performed a silent refresh", 1, refreshCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGetTokenRefreshesWithoutPrompting() throws Exception {
        assertMemoryLeak(() -> {
            // getToken() returns the cached token, silently refreshes it when it expires, and never
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
                // before any sign-in, getToken() must not prompt - it throws
                assertOidcFails(auth::getToken, "no token", "expected getToken() to fail before sign-in");
                // sign in once interactively
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the cached token is expired, so getToken() refreshes silently
                Assert.assertEquals("ACCESS-2", auth.getToken());
                // now make the refresh fail; getToken() must throw, not start the device flow
                refreshOk.set(false);
                expireCachedToken(auth);
                assertOidcFails(auth::getToken, "interactive sign-in",
                        "expected getToken() to fail when the refresh is rejected");
                // the device flow ran exactly once (the initial signIn), and the user was prompted once
                Assert.assertEquals(1, deviceCalls.get());
                Assert.assertEquals(1, promptCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBlankServedTokenFromWireIsNotServed() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or broken IdP returns a whitespace-only access token on the grant: it is non-empty and
            // passes the control/non-ASCII char check vacuously (space is 0x20), but must NOT be cached and
            // served as a blank "Bearer " header (which only draws a 401). storeTokens folds a blank served
            // token to absent, so signIn() fails with the actionable "no access_token" rather than serving "   "
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("   ", null, "REFRESH-1", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                assertOidcFails(auth::signIn, "no access_token",
                        "expected signIn() to reject a blank served token from the wire");
            }
        });
    }

    @Test(timeout = 30_000)
    public void testBlankTokenFromRefreshFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // a non-conformant IdP answers a SILENT REFRESH with a 2xx carrying a whitespace-only access token.
            // The refresh gate (hasRequiredToken) must treat it as absent with the same Chars.isBlank contract
            // storeTokens uses, so tryRefresh() reports failure and signIn() falls back to the interactive device
            // flow - rather than caching a token storeTokens then nulls, which would make signIn() throw "no
            // access_token" while a fresh interactive sign-in was still possible.
            AtomicInteger deviceCodePolls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // blank served token on refresh: the gate must fall back, not cache-and-serve it
                    return MockOidcServer.json(200, tokenJson("   ", null, null, 3600));
                }
                // the device-code grant: the first poll mints the initial short-lived token; the second is the
                // interactive fallback after the blank refresh and mints a fresh, usable one
                return deviceCodePolls.incrementAndGet() == 1
                        ? MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 60))
                        : MockOidcServer.json(200, tokenJson("ACCESS-FALLBACK", null, "REFRESH-2", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth); // force the silent-refresh path on the next sign-in
                // with the blank-refresh gate fixed, signIn() falls back to the device flow instead of throwing
                Assert.assertEquals("ACCESS-FALLBACK", auth.signIn());
                Assert.assertEquals("the interactive device flow must run again as the fallback",
                        2, deviceCodePolls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testGroupsInTokenButNoIdTokenFails() throws Exception {
        assertMemoryLeak(() -> {
            // groups encoded in token, but the IdP returns only an access token on the initial grant
            // (e.g. the requested scope omitted openid); signIn() must fail with an actionable message
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-ONLY", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, true, noopPrompt())) {
                assertOidcFails(auth::signIn, "no id_token");
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
                Assert.assertEquals("ID-X", auth.signIn());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testHttpSenderProviderFailureAfterFlushDoesNotCorruptSender() throws Exception {
        assertMemoryLeak(() -> {
            // regression: the per-request token must be pulled lazily when a row starts, never eagerly when
            // the post-flush request is rebuilt. A provider that throws on a later pull (e.g.
            // OidcDeviceAuth::getToken when a refresh fails) must NOT turn an already-successful
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
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("could not be refreshed"));
                    Assert.assertTrue("the provider failure must be retained as the cause",
                            e.getCause() instanceof OidcAuthException);
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
                assertOidcFails(auth::signIn, "incomplete device authorization");
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIdpEndpointsRequireHttpsExceptLoopback() throws Exception {
        assertMemoryLeak(() -> {
            // a non-loopback http identity-provider endpoint carries the device code and refresh token in
            // cleartext, so it must be refused
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://idp.example/device")
                    .tokenEndpoint("https://idp.example/token")
                    .build()
            ) {
                Assert.fail("expected the http device authorization endpoint to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("device authorization endpoint"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
            }
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("https://idp.example/device")
                    .tokenEndpoint("http://idp.example/token")
                    .build()
            ) {
                Assert.fail("expected the http token endpoint to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("token endpoint"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
            }
            // allowInsecureTransport must NOT relax the identity provider endpoints (unlike the QuestDB
            // link), matching the Python client; a non-loopback http endpoint stays rejected, and the
            // error says so
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://idp.example/device")
                    .tokenEndpoint("http://idp.example/token")
                    .allowInsecureTransport(true)
                    .build()
            ) {
                Assert.fail("allowInsecureTransport must not relax a non-loopback http identity provider endpoint");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("insecure http"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("allowInsecureTransport relaxes only the QuestDB"));
            }
            // loopback http is allowed without any flag: the request never leaves the host
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://127.0.0.1:9999/device")
                    .tokenEndpoint("http://127.0.0.1:9999/token")
                    .build()
            ) {
                // accepted: loopback endpoints never put the device code or refresh token on the network
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingAcceptsEndpointsUnderIssuerPath() throws Exception {
        assertMemoryLeak(() -> {
            // a path-based identity provider (Keycloak-style /realms/{realm}): the issuer carries a path and
            // /settings advertises the endpoints under it, so the flow completes
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                if (SETTINGS_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{\"config\":{"
                            + "\"acl.oidc.enabled\":true,"
                            + "\"acl.oidc.client.id\":\"questdb\","
                            + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/acme/token") + "\","
                            + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl("/realms/acme/device") + "\""
                            + "}}");
                }
                if ("/realms/acme/device".equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-REALM", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("/realms/acme")))) {
                    Assert.assertEquals("ACCESS-REALM", auth.signIn());
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsEncodedSlash() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint hides an extra path segment behind a %2f-encoded slash; decoding it would
            // split acme%2fevil into acme/evil and slip the "/realms/acme" scope, so an encoded path separator
            // must be rejected outright
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.client.id\":\"questdb\","
                        + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/acme/token") + "\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl("/realms/acme%2fevil/device") + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("/realms/acme")))) {
                    Assert.fail("expected the %2f-encoded device endpoint to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("not under the pinned issuer"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsEncodedTraversal() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint hides a parent traversal as %2e%2e; decoding must unmask it and reject it,
            // since the server would normalize /realms/acme/../evil to a different realm
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.client.id\":\"questdb\","
                        + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/acme/token") + "\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl("/realms/acme/%2e%2e/evil/device") + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("/realms/acme")))) {
                    Assert.fail("expected the encoded ..-traversal device endpoint to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("not under the pinned issuer"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsMatrixParamTraversal() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint hides a parent traversal as an RFC 3986 ";matrix" segment (..;): a server or
            // proxy that strips matrix params resolves /realms/acme/..;/evil to /realms/evil, a DIFFERENT realm.
            // The plain "." / ".." dot-segment check does not match "..;", so the check must strip the ";suffix"
            // first and reject it - the origin pin alone cannot stop a sibling-tenant redirect on one host.
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.client.id\":\"questdb\","
                        + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/acme/token") + "\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl("/realms/acme/..;/evil/device") + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("/realms/acme")))) {
                    Assert.fail("expected the ..;-matrix-param traversal device endpoint to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("not under the pinned issuer"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsSiblingRealm() throws Exception {
        assertMemoryLeak(() -> {
            // a tampered /settings advertises a token endpoint under a DIFFERENT realm on the same origin; the
            // origin check alone would accept it, but path scoping must reject it
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, "{\"config\":{"
                        + "\"acl.oidc.enabled\":true,"
                        + "\"acl.oidc.client.id\":\"questdb\","
                        + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/evil/token") + "\","
                        + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl("/realms/acme/device") + "\""
                        + "}}");
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(server.httpUrl("/realms/acme")))) {
                    Assert.fail("expected the off-path (sibling realm) token endpoint to be rejected");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("not under the pinned issuer"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsRawDotSegments() throws Exception {
        assertMemoryLeak(() -> {
            // A RAW (unencoded) ".." or "." segment carries no '%' or '\\' and no '?'/'#'/control, so it slips
            // every earlier gate and reaches the dot-segment scan - the only cases that do. A lenient server
            // normalizes .../realms/acme/../evil/device into a different realm, so the pin must reject it.
            assertIssuerScopeAccepts("/realms/acme/device");
            assertIssuerScopeRejects("/realms/acme/../evil/device");
            assertIssuerScopeRejects("/realms/acme/./../evil/device");
            assertIssuerScopeRejects("/realms/./acme/device");
        });
    }

    @Test(timeout = 30_000)
    public void testIssuerPathScopingRejectsSplitEncodedAndBackslashSeparators() throws Exception {
        assertMemoryLeak(() -> {
            // An encoded path separator can hide behind a SPLIT encoding (%2%66 -> %2f -> '/'), a double
            // encoding (%252f), or a backslash that decodePathSegments folds to '/'. Each lets an extra
            // segment masquerade as being under the issuer path while a different raw path travels on the
            // wire. Overlong UTF-8 (%c0%ae, %e0%80%ae) and an IIS-style %u002e encode a '.' that a permissive
            // server resolves but a byte-oriented decode leaves as high bytes, so any '%' in an endpoint path
            // is refused outright.
            assertIssuerScopeAccepts("/realms/acme/protocol/device");
            assertIssuerScopeRejects("/realms/acme%2%66evil/device");
            assertIssuerScopeRejects("/realms/acme%252fevil/device");
            assertIssuerScopeRejects("/realms/acme\\evil/device");
            assertIssuerScopeRejects("/realms/acme%5cevil/device");
            assertIssuerScopeRejects("/realms/acme%5Cevil/device");
            assertIssuerScopeRejects("/realms/acme/%c0%ae%c0%ae/evil/device");
            assertIssuerScopeRejects("/realms/acme/%e0%80%ae%e0%80%ae/evil/device");
            assertIssuerScopeRejects("/realms/acme/%u002e%u002e/evil/device");
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
            String json = "{\"id_token\":\"" + TestUtils.repeat("a", 4000) + "\"}";
            int len = json.length();
            int split = "{\"id_token\":\"".length() + 1300; // boundary inside the value, past the old 1024 limit
            long address = TestUtils.toMemory(json);
            try {
                try {
                    parseSplitValue(1024, address, split, len);
                    Assert.fail("the original 1024-byte cache limit must reject a split multi-KB token value");
                } catch (JsonException expected) {
                    Assert.assertTrue(expected.getFlyweightMessage().toString(),
                            expected.getFlyweightMessage().toString().contains("String is too long"));
                }
                // the sizing OidcDeviceAuth now uses parses the same split value
                parseSplitValue(1 << 20, address, split, len);
            } finally {
                Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPlaintextIdpEndpointIsAllowedOnlyForLoopbackHosts() {
        // The rule the loopback classifier exists to serve: the identity provider endpoints must use https,
        // because the device code and the refresh token travel over them - EXCEPT to a loopback host, where
        // the request never leaves the machine. Driven through build(), which does no network I/O, rather
        // than by reflecting on the private classifier: this asserts the outcome a user actually gets, and
        // it survives that predicate being renamed, inlined or replaced.
        //
        // Both endpoints use the same host because build() also requires them to share an origin; that check
        // is not what is under test here, it just has to be satisfied for the loopback rows to reach a verdict.
        String[] loopback = {
                "localhost", "LOCALHOST", "LocalHost",
                "127.0.0.1", "127.0.0.0", "127.1.2.3", "127.255.255.255", "127.0.0.255"
        };
        for (String host : loopback) {
            try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://" + host + "/device")
                    .tokenEndpoint("http://" + host + "/token")
                    .build()) {
                Assert.assertNotNull("plaintext to a loopback host must be allowed: [" + host + ']', auth);
            }
        }

        // Everything else must be refused over plaintext, so the MITM pin fires. A classifier that accepted
        // any of these would silently send a device code and a refresh token across the network in the clear.
        String[] notLoopback = {
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
                "0.0.0.0", "10.0.0.1", "192.168.0.1",
                // A name is accepted on the strength of what it RESOLVES to, not how it is spelt - RFC 6761
                // says localhost must be loopback, but a host with no /etc/hosts entry leaves that to DNS.
                // One that does not resolve must fail CLOSED, i.e. be treated as non-loopback. (The hostile
                // half - localhost resolving off loopback - needs the host's resolver rewritten, so it is
                // unasserted by design rather than by omission.)
                "no-such-host.invalid"
        };
        for (String host : notLoopback) {
            assertBuildFails("http://" + host + "/device", "http://" + host + "/token", "use an https url");
        }

        // Two forms the classifier never sees, because the endpoint parser rejects them first. Asserted here
        // so the list above is not silently assumed to cover them.
        assertBuildFails("http:///device", "http:///token", "the host is empty");
        assertBuildFails("http://[::1]:9000/device", "http://[::1]:9000/token",
                "IPv6 literal hosts are not supported");
    }

    @Test(timeout = 30_000)
    public void testPlaintextSettingsWithAdvertisedEndpointsRequiresPin() throws Exception {
        // The "127.1" reachability trick below depends on the OS resolver expanding the abbreviated IPv4
        // form to 127.0.0.1 (inet_aton, on Linux/macOS). Windows getaddrinfo - which the native HTTP client
        // resolves through - does not accept the short form, so the loopback mock is unreachable there.
        Assume.assumeTrue("requires inet_aton-style short-form IPv4 resolution, unavailable on Windows", Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            // the end-to-end firing path of the plaintext-channel MITM pin, which a 127.0.0.1-bound mock
            // cannot otherwise reach: a non-loopback http /settings that advertises BOTH endpoints (so the
            // missing-endpoint discovery pin does not apply) must be refused unless the identity provider is
            // pinned out of band - otherwise a tampered response could route the device code and refresh token
            // to an attacker. Reaching the mock through "127.1" is the trick: the OS resolver expands the short
            // form to 127.0.0.1 so the loopback mock answers, but the loopback classifier deliberately rejects
            // the short form, so the server host is non-loopback and the pin fires.
            AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                MockOidcServer server = serverRef.get();
                return MockOidcServer.json(200, settingsJson(true, true, server.httpUrl(TOKEN_PATH), server.httpUrl(DEVICE_PATH)));
            };
            try (MockOidcServer server = new MockOidcServer(handler)) {
                serverRef.set(server);
                String questdbUrl = "http://127.1:" + server.port();
                // without an out-of-band pin the plaintext channel is untrusted: the pin fires
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(questdbUrl, insecure())) {
                    Assert.fail("expected the plaintext-channel pin to reject /settings-supplied endpoints without a pin");
                } catch (OidcAuthException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("reached over insecure http"));
                }
                // pinning the issuer to the advertised endpoints' origin satisfies the pin over the very same
                // plaintext channel, so construction succeeds - proving the pin, not some unrelated rejection,
                // is what gated the unpinned call above
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(questdbUrl, insecure().issuer(server.httpUrl("")))) {
                    Assert.assertNotNull(auth);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRejectedBuildDoesNotLeakNativeMemory() throws Exception {
        // A build rejected during validation must not leak. build() parses and validates every endpoint
        // BEFORE the constructor runs, and the constructor allocates the native JSON lexer LAST (after
        // urlEncode and the TokenStoreKey build, either of which can throw), so a rejected build never
        // allocates the lexer and the never-returned instance cannot be closed to free it. Use a
        // parseable-but-rejected config - endpoints that parse cleanly but fail the https requirement - so the
        // rejection lands AFTER endpoint parsing, exercising more of build() than a syntactically bad url
        // would. testSuccessfulBuildAndCloseDoNotLeakNativeMemory covers the complementary lexer-allocated
        // path. assertMemoryLeak guards every tag; the parser-tag assertion stays because it names the buffer.
        assertMemoryLeak(() -> {
            long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
            try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                    .clientId("c")
                    .deviceAuthorizationEndpoint("http://idp.example/device") // parses fine, but plaintext http to a non-loopback host
                    .tokenEndpoint("https://idp.example/token")
                    .allowInsecureTransport(false)
                    .build()
            ) {
                Assert.fail("expected the https requirement to reject the plaintext device endpoint");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("use an https url"));
            }
            Assert.assertEquals("a rejected build must not leak the JSON lexer native buffer",
                    parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
        });
    }

    @Test(timeout = 30_000)
    public void testSuccessfulBuildAndCloseDoNotLeakNativeMemory() throws Exception {
        // The complement to the rejected-build case: a SUCCESSFUL build is the only path that allocates the
        // native JSON lexer, so this is the block that actually exercises a lexer-allocated instance, and
        // close() must free it. Loop a few build->close cycles so any per-cycle leak accrues, then assert the
        // parser tag returns to its baseline. build() does no network I/O (discovery is separate), so valid
        // co-located https endpoints construct offline. assertMemoryLeak guards every tag; the parser-tag
        // assertion stays because it names the buffer.
        assertMemoryLeak(() -> {
            long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
            for (int i = 0; i < 4; i++) {
                try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                        .clientId("c")
                        .deviceAuthorizationEndpoint("https://idp.example/device")
                        .tokenEndpoint("https://idp.example/token")
                        .build()) {
                    Assert.assertNotNull(auth);
                }
            }
            Assert.assertEquals("close() must free the JSON lexer native buffer allocated by a successful build",
                    parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
        });
    }

    @Test(timeout = 30_000)
    public void testNoAccessTokenWhenGroupsDisabledFails() throws Exception {
        assertMemoryLeak(() -> {
            // groups not in token, but the IdP returns only an id token; signIn() must fail
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson(null, "ID-ONLY", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                assertOidcFails(auth::signIn, "no access_token");
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
                assertOidcFails(auth::signIn, "unexpected response from the device authorization endpoint",
                        "expected the non-2xx device authorization response to be rejected");
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
                // null is absent, so a 2xx with no token is a definitive but malformed answer. The token the
                // call would have served, had it wrongly served the literal "null", is in the failure message
                // assertOidcFails builds.
                assertOidcFails(auth::signIn, "unexpected response",
                        "a JSON null access_token must not be served as the literal token \"null\"");
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
                Assert.assertEquals("ACCESS-OK", auth.signIn());
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
                Assert.assertEquals("ACCESS-NP", auth.signIn());
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
                OidcAuthException e = assertOidcFails(auth::signIn, "access_denied");
                Assert.assertEquals("access_denied", e.getOauthError());
                String msg = e.getMessage();
                assertNoUnsafeDisplayChars(msg);
                Assert.assertTrue(msg, msg.contains("deniedreversedend")); // readable text survives, controls gone
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
                OidcAuthException e = assertOidcFails(auth::signIn, "access_denied");
                Assert.assertEquals("access_denied", e.getOauthError());
                String msg = e.getMessage();
                assertNoControlChars(msg);
                Assert.assertTrue(msg, msg.contains("FAKE: paste your token")); // readable text survives
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
                Assert.assertEquals("ACCESS-CLAMP", auth.signIn());
                DeviceAuthorizationChallenge challenge = shown.get();
                Assert.assertNotNull(challenge);
                // the absurd interval/expires_in are clamped to the documented maxima: the poll interval to
                // MAX_POLL_INTERVAL_SECONDS (60) and the device-code lifetime to MAX_DEVICE_CODE_TTL_SECONDS (1800)
                Assert.assertEquals(60, challenge.getIntervalSeconds());
                Assert.assertEquals(1800, challenge.getExpiresInSeconds());
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
    public void testPollAbortDropsDirtyConnectionAndReconnects() throws Exception {
        assertMemoryLeak(() -> {
            // the token endpoint stalls the body on the first poll, so the bounded read aborts with the
            // response half-read and unconsumed bytes left in the cached keep-alive connection. The poll loop
            // must drop that connection and reconnect for the next poll, not reuse it: the stalled mock thread
            // never reads a reused connection, so reusing it would leave every later poll unanswered until the
            // device code expires (and, for a non-stalled dirty connection, would mis-frame the next response
            // against this one's leftovers). With the reconnect, the second poll reaches a fresh connection and
            // succeeds. Without the fix this test hangs until the 10s device-code lifetime and signIn throws.
            AtomicInteger tokenCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    // short lifetime, well under the 30s mock stall and the 30s test timeout, so the no-fix
                    // failure (poll the dirty connection until expiry) surfaces deterministically and fast
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 10));
                }
                if (tokenCalls.getAndIncrement() == 0) {
                    return MockOidcServer.stall();
                }
                return MockOidcServer.json(200, tokenJson("ACCESS-RECONNECTED", null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = OidcDeviceAuth.builder()
                         .clientId("questdb")
                         .deviceAuthorizationEndpoint(server.httpUrl(DEVICE_PATH))
                         .tokenEndpoint(server.httpUrl(TOKEN_PATH))
                         .httpTimeoutMillis(1_000) // abort the stalled body read quickly
                         .allowInsecureTransport(true)
                         .prompt(noopPrompt())
                         .build()) {
                Assert.assertEquals("ACCESS-RECONNECTED", auth.signIn());
                Assert.assertEquals(2, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPollIntervalClampedTo60() throws Exception {
        assertMemoryLeak(() -> {
            // the identity-provider-reported poll interval is capped at 60s (matching the Python client); the
            // clamped value is the one shown to the user and used between polls. A short-lived device code
            // ends the flow quickly via timeout, once the interval has been captured by the prompt.
            AtomicReference<DeviceAuthorizationChallenge> shown = new AtomicReference<>();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(999, 2));
                }
                return MockOidcServer.json(400, "{\"error\":\"authorization_pending\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, shown::set)) {
                assertOidcFails(auth::signIn, "device code expired", "expected the device code to expire");
                Assert.assertEquals(60, shown.get().getIntervalSeconds());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRateLimited429WithTerminalErrorAbortsImmediately() throws Exception {
        assertMemoryLeak(() -> {
            // a 429 that ALSO carries a terminal OAuth error must fail fast on the error, not back off and poll
            // to the device-code deadline: pollOnce handles the OAuth error before the 429 rate-limit backoff
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 5));
                }
                return MockOidcServer.json(429, "{\"error\":\"access_denied\",\"error_description\":\"the user declined\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.signIn();
                    Assert.fail("expected the terminal OAuth error to abort despite the 429 status");
                } catch (OidcAuthException e) {
                    Assert.assertEquals("access_denied", e.getOauthError());
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("device code expired"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRateLimitedTokenEndpointBacksOffInsteadOfFailingFast() throws Exception {
        assertMemoryLeak(() -> {
            // HTTP 429 is a transient backoff (poll slower, keep polling), matching the Python client, not a
            // terminal rejection. The token endpoint always returns 429, so the flow ends only when the
            // short-lived device code expires - proving polling continued rather than failing fast.
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 4));
                }
                return MockOidcServer.json(429, "{}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "device code expired",
                        "expected the device code to expire while the token endpoint kept returning 429");
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("rejected the request"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPersistentTransportFailureKeepsPollingToDeadline() throws Exception {
        assertMemoryLeak(() -> {
            // the device endpoint works, but the (co-located) token endpoint drops the connection on every
            // poll. Matching the Python client, a transport failure is transient - the user may already have
            // authorized - so polling continues until the device code expires rather than failing fast. The
            // endpoints share one origin so the build-time co-location check passes; the mock simulates the
            // unreachable token endpoint by dropping the connection.
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 3));
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
                    auth.signIn();
                    Assert.fail("expected the device code to expire while the token endpoint kept dropping");
                } catch (OidcAuthException e) {
                    // polled to the deadline (device code expired), not a fast transport abort
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("device code expired"));
                    Assert.assertFalse(e.getMessage(), e.getMessage().contains("unreachable"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPersistent5xxDuringPollingKeepsPollingToDeadline() throws Exception {
        assertMemoryLeak(() -> {
            // a 5xx from the token endpoint is a transient server/gateway condition: keep polling to the
            // device-code deadline rather than failing fast, matching the Python client
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 3));
                }
                return MockOidcServer.json(503, "{}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "device code expired",
                        "expected the device code to expire while the token endpoint returned 503");
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("rejected the request"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTerminal4xxDuringPollingFailsFast() throws Exception {
        assertMemoryLeak(() -> {
            // a 4xx from the token endpoint with no OAuth error (e.g. a WAF or proxy rejection) is terminal:
            // fail fast rather than poll on to a misleading "device code expired", matching the Python client
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(403, "{}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "rejected the request",
                        "expected a terminal 4xx to fail fast");
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("device code expired"));
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the refresh is rejected, so the flow re-runs the interactive sign-in
                Assert.assertEquals("ACCESS-2", auth.signIn());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshedTokenWithControlCharFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // A silent refresh whose 200 response carries a served token with a control character - here an
            // escaped \r that JsonLexer now decodes into a real CR byte - must be rejected by storeTokens ->
            // validateTokenChars, and tryRefresh must SWALLOW that rejection and fall back to the interactive
            // device flow rather than let it propagate out of signIn()/getToken(). Guards the tryRefresh
            // storeTokens try/catch: without it, this signIn() throws instead of returning the fallback token.
            AtomicInteger deviceCalls = new AtomicInteger();
            AtomicInteger deviceCodeGrants = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                if (body.contains("grant_type=refresh_token")) {
                    // valid-JSON 200, but the access_token carries an escaped CR (\r on the wire); the served
                    // kind is validated, so validateTokenChars must reject it before it is cached
                    return MockOidcServer.json(200, tokenJson("ACCESS\\r2", null, "REFRESH-2", 3600));
                }
                // the initial device-code grant uses a short TTL so the next signIn() triggers a refresh
                if (deviceCodeGrants.getAndIncrement() == 0) {
                    return MockOidcServer.json(200, tokenJson("ACCESS-1", null, "REFRESH-1", 1));
                }
                // the fallback interactive grant, after the poisoned refresh is rejected
                return MockOidcServer.json(200, tokenJson("ACCESS-3", null, "REFRESH-3", 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the refresh returns a control-char token -> rejected -> fall back to a fresh interactive sign-in
                Assert.assertEquals("ACCESS-3", auth.signIn());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback after the rejected refresh)",
                        2, deviceCalls.get());
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // first refresh omits refresh_token, so REFRESH-1 must be kept
                Assert.assertEquals("ACCESS-R1", auth.signIn());
                expireCachedToken(auth);
                // second refresh must still present the retained REFRESH-1 (asserted in the handler)
                Assert.assertEquals("ACCESS-R2", auth.signIn());
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the refresh carries an error+token, so the client must ignore the smuggled token and
                // re-run the interactive flow
                Assert.assertEquals("ACCESS-2", auth.signIn());
                Assert.assertEquals("the interactive flow must run twice (initial + fallback)", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testRefreshWithoutIdTokenFallsBackToInteractiveFlow() throws Exception {
        assertMemoryLeak(() -> {
            // groups are encoded in the token (the default enterprise config), so signIn() serves the
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
                Assert.assertEquals("ID-1", auth.signIn());
                expireCachedToken(auth);
                // the refresh returns no id_token, so the flow falls back to interactive sign-in and
                // returns the fresh id token instead of throwing "returned no id_token"
                Assert.assertEquals("ID-2", auth.signIn());
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
                Assert.assertEquals("ACCESS-RECOVERED-5XX", auth.signIn());
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
                Assert.assertEquals("ACCESS-1", auth.signIn());
                expireCachedToken(auth);
                // the cached token is expired, so the second call refreshes silently
                Assert.assertEquals("ACCESS-2", auth.signIn());
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
                Assert.assertEquals("ACCESS-S", auth.signIn());
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
                    auth.signIn();
                    Assert.fail("expected the stalled body read to abort");
                } catch (OidcAuthException e) {
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    // aborted on the configured ~1s OIDC timeout: not instantly (which would be a different
                    // failure path) and not on the 600s HttpClient default (or an indefinite wedge). The window
                    // proves the 1s timeout fired, with generous headroom for a slow CI host
                    Assert.assertTrue("aborted too fast to be the 1s timeout: " + elapsedMillis + "ms", elapsedMillis >= 500);
                    Assert.assertTrue("aborted too slowly for the 1s timeout: " + elapsedMillis + "ms", elapsedMillis < 5_000);
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
                assertOidcFails(auth::signIn, "timed out", "expected a timeout");
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
                    auth.signIn();
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
                Assert.assertEquals("ACCESS-C", auth.signIn());
                Assert.assertEquals("ACCESS-C", auth.signIn());
                Assert.assertEquals("ACCESS-C", auth.signIn());
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
                // a 4xx (terminal) carrying a token but malformed JSON: the parser fails, and the raw body
                // (with the token) must NOT be echoed into the exception message
                return MockOidcServer.json(400, "{\"access_token\":\"" + secret + "\" not-valid-json}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "httpStatus=");
                Assert.assertFalse("the token must not leak into the message: " + e.getMessage(),
                        e.getMessage().contains(secret));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenResponseExpiresInIsClamped() throws Exception {
        assertMemoryLeak(() -> {
            // an absurd token-response expires_in (here Integer.MAX_VALUE, ~68 years) must be clamped to
            // MAX_EXPIRES_IN_SECONDS (1h) like the device-side value, so the client does not trust a stale
            // cached token for decades (the server still enforces the real expiry).
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
                         .build()) {
                long before = System.currentTimeMillis();
                Assert.assertEquals("ACCESS-OK", auth.signIn());
                long after = System.currentTimeMillis();
                Assert.assertEquals("first sign-in runs the device flow once", 1, deviceCalls.get());

                // the cached expiry must be ~1h out (the clamp), not ~68 years
                long maxLifetimeMillis = 3600L * 1000L;
                long expiresAt = readExpiresAtMillis(auth);
                Assert.assertTrue("expiry must be clamped to <= 1h ahead, was " + (expiresAt - before) + "ms ahead",
                        expiresAt <= after + maxLifetimeMillis);
                Assert.assertTrue("expiry must be ~1h ahead (the clamp), was " + (expiresAt - after) + "ms ahead",
                        expiresAt >= before + maxLifetimeMillis - 5_000L);

                // once the clamped token is past expiry, with no refresh token signIn() re-runs the device flow
                expireCachedToken(auth);
                Assert.assertEquals("ACCESS-OK", auth.signIn());
                Assert.assertEquals("expired clamped token forces a fresh sign-in", 2, deviceCalls.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenResponseExpiresInZeroUsesDefaultTtl() throws Exception {
        assertMemoryLeak(() -> {
            // a token response with a non-positive expires_in (here 0) must fall back to
            // DEFAULT_TOKEN_TTL_SECONDS (5 min), not be treated as already-expired or cached forever.
            // testTokenResponseExpiresInIsClamped covers the absurd-large end; this covers the <= 0 default.
            AtomicInteger deviceCalls = new AtomicInteger();
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    deviceCalls.incrementAndGet();
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                // no refresh_token, so an expired cache forces a fresh device flow rather than a silent refresh
                return MockOidcServer.json(200, tokenJson("ACCESS-DEF", null, null, 0)); // expires_in = 0
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                long before = System.currentTimeMillis();
                Assert.assertEquals("ACCESS-DEF", auth.signIn());
                long after = System.currentTimeMillis();
                Assert.assertEquals("first sign-in runs the device flow once", 1, deviceCalls.get());

                // the cached expiry must be ~5min out (the default), neither ~now (treated as expired) nor far
                long defaultTtlMillis = 300L * 1000L;
                long expiresAt = readExpiresAtMillis(auth);
                Assert.assertTrue("expiry must be ~5min ahead (the default), was " + (expiresAt - after) + "ms ahead",
                        expiresAt >= before + defaultTtlMillis - 5_000L);
                Assert.assertTrue("expiry must be ~5min ahead (the default), not longer, was " + (expiresAt - before) + "ms ahead",
                        expiresAt <= after + defaultTtlMillis);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenUnderNonSuccessStatusIsNotAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // RFC 6749 5.1: a token must come from a 2xx response. A token under a non-2xx status with no
            // OAuth error is a malformed or hostile answer; the client must not cache it - a 4xx is a
            // terminal rejection that fails fast rather than trusting the token
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"access_token\":\"SHOULD-NOT-BE-USED\"}");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "rejected the request",
                        "expected a token under a 400 to be rejected, not accepted");
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("SHOULD-NOT-BE-USED"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenWithControlCharsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // a hostile or man-in-the-middled identity provider returns an access token whose JSON value carries
            // an escaped CR/LF; the lexer decodes it to real control bytes, which - sent verbatim in the
            // Authorization header to the trusted QuestDB server - would inject into the request line. storeTokens
            // must reject the token rather than cache and serve it, and must not leak the token into the message
            String injected = "header.payload" + jsonUnicodeEscape(0x0d) + jsonUnicodeEscape(0x0a) + "X-Injected:1";
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson(injected, null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "disallowed control or non-ASCII",
                        "expected a token with control characters to be rejected");
                // the token bytes must never leak into the message
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("X-Injected"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTokenWithNonAsciiCharRejected() throws Exception {
        assertMemoryLeak(() -> {
            // the > 0x7e arm of the token guard (testTokenWithControlCharsRejected covers the < 0x20 arm):
            // a non-ASCII char (here U+00E9, not a control char) in the access token would be silently
            // truncated to one byte by the ASCII Authorization-header writer, yielding a corrupt credential.
            // storeTokens must reject it, and must not leak the token into the message
            String injected = "header.payload" + jsonUnicodeEscape(0x00e9) + "SHOULD-NOT-LEAK"; // e-acute, > 0x7e
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(200, tokenJson(injected, null, null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                OidcAuthException e = assertOidcFails(auth::signIn, "disallowed control or non-ASCII",
                        "expected a token with a non-ASCII character to be rejected");
                // the token bytes must never leak into the message
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("SHOULD-NOT-LEAK"));
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
                Assert.assertEquals("ACCESS-RECOVERED", auth.signIn());
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
                try (OidcDeviceAuth ignored = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure())) {
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
            // rejected (parseLast catches the dangling value), not silently treated as no token. A 4xx makes
            // the parse failure terminal so it surfaces immediately (a malformed 2xx is retried as transient).
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, deviceAuthorizationJson(1, 300));
                }
                return MockOidcServer.json(400, "{\"access_token\":\"abc");
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                assertOidcFails(auth::signIn, "could not parse");
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
                assertOidcFails(auth::signIn, "unexpected response");
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUnreachableDeviceEndpointThrowsOidcAuthException() throws Exception {
        assertMemoryLeak(() -> {
            // a connection failure to the device endpoint must surface as OidcAuthException (signIn's
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
                auth.signIn();
                Assert.fail("expected an OidcAuthException");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("device authorization endpoint"));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUseAfterCloseThrowsClearly() {
        // calling signIn()/clearCache() after close() must fail with a clear "closed" error rather than
        // NPE on the freed JSON lexer or resurrect (and leak) a fresh native HTTP client
        long parserMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);
        // close() is the subject under test, so it is called explicitly mid-body; the try-with-resources
        // close at scope exit is a harmless idempotent second close that also covers an early assertion throw
        try (OidcDeviceAuth auth = OidcDeviceAuth.builder()
                .clientId("c")
                .deviceAuthorizationEndpoint("https://idp.example/device")
                .tokenEndpoint("https://idp.example/token")
                .build()
        ) {
            auth.close();
            assertOidcFails(auth::signIn, "closed", "expected signIn() after close() to be rejected");
            try {
                auth.clearCache();
                Assert.fail("expected clearCache() after close() to be rejected");
            } catch (OidcAuthException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("closed"));
            }
            // signIn() must reject before resurrecting a native HTTP client, and close() must have freed
            // the JSON lexer, so the parser-tag memory returns to its pre-construction level
            Assert.assertEquals("a closed instance must not leak or resurrect native memory",
                    parserMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
        }
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
                Assert.assertEquals("ACCESS-ALIAS", auth.signIn());
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
            // requested scope omitted openid). signIn() must fail the first call, then re-run the
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
                assertOidcFails(auth::signIn, "no id_token", "expected an OidcAuthException on the first call");
                // the unusable grant must NOT be cached as valid: the next call re-runs the flow and succeeds
                Assert.assertEquals("ID-2", auth.signIn());
                Assert.assertEquals("the interactive flow must run twice (failed first, recovered second)", 2, deviceCalls.get());
            }
        });
    }

    private static void assertBuildFails(String deviceEndpoint, String tokenEndpoint, String expectedMessage) {
        try (OidcDeviceAuth ignored = OidcDeviceAuth.builder()
                .clientId("c")
                .deviceAuthorizationEndpoint(deviceEndpoint)
                .tokenEndpoint(tokenEndpoint)
                .build()
        ) {
            Assert.fail("expected build to fail for device=" + deviceEndpoint + " token=" + tokenEndpoint);
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    /**
     * Fails unless NO sink or String reachable from {@code instance} - its own fields, and the fields of any
     * client object they point at - still carries {@code secret}. A StringSink is read through its whole
     * backing array, not just up to its write position, because that tail is precisely what a plain clear()
     * leaves behind.
     */
    private static void assertHoldsNowhere(Object instance, String secret) throws Exception {
        String holder = findHolderOf(instance, secret);
        Assert.assertNull("close() left \"" + secret + "\" readable in " + holder, holder);
    }

    private static void assertHoldsSomewhere(Object instance, String secret) throws Exception {
        Assert.assertNotNull("the state this test is about was never there: no field holds \"" + secret + '"',
                findHolderOf(instance, secret));
    }

    /**
     * Drives one issuer-path case through the PUBLIC path a user takes: a QuestDB {@code /settings} that
     * advertises {@code devicePath} while the caller pins the issuer to {@code /realms/acme}. Asserts the
     * outcome a caller sees - fromQuestDB throwing - rather than the return value of the private scan, so a
     * rename or an inline of that scan leaves the coverage intact. The sibling scenario tests
     * (testIssuerPathScopingRejectsEncodedSlash and friends) use the same shape; this exists so the encoding
     * table can stay a table.
     */
    private static void assertIssuerScope(String devicePath, boolean accepted) throws Exception {
        AtomicReference<MockOidcServer> serverRef = new AtomicReference<>();
        MockOidcServer.Handler handler = (method, path, body) -> {
            MockOidcServer server = serverRef.get();
            return MockOidcServer.json(200, "{\"config\":{"
                    + "\"acl.oidc.enabled\":true,"
                    + "\"acl.oidc.client.id\":\"questdb\","
                    + "\"acl.oidc.token.endpoint\":\"" + server.httpUrl("/realms/acme/token") + "\","
                    + "\"acl.oidc.device.authorization.endpoint\":\"" + server.httpUrl(devicePath) + "\""
                    + "}}");
        };
        try (MockOidcServer server = new MockOidcServer(handler)) {
            serverRef.set(server);
            final String issuer = server.httpUrl("/realms/acme");
            if (accepted) {
                try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(issuer))) {
                    Assert.assertNotNull("an endpoint genuinely under the issuer path must be accepted: "
                            + devicePath, auth);
                }
            } else {
                assertOidcFails(() -> OidcDeviceAuth.fromQuestDB(server.httpUrl(""), insecure().issuer(issuer)),
                        "not under the pinned issuer",
                        "an endpoint that escapes the issuer path must be rejected: " + devicePath);
            }
        }
    }

    private static void assertIssuerScopeAccepts(String devicePath) throws Exception {
        assertIssuerScope(devicePath, true);
    }

    private static void assertIssuerScopeRejects(String devicePath) throws Exception {
        assertIssuerScope(devicePath, false);
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

    /**
     * Asserts that {@code call} - a {@code signIn()}, a {@code getToken()} or a discovery that must not
     * succeed - throws an {@link OidcAuthException} whose message carries {@code expectedMessage}, and hands
     * that exception back so a caller with more to check keeps asserting on it. Same idiom as
     * {@link #assertBuildFails}, applied to the seven-line try/fail/catch this file used to stamp out at
     * roughly thirty sites.
     */
    private static OidcAuthException assertOidcFails(Supplier<?> call, String expectedMessage) {
        return assertOidcFails(call, expectedMessage, "the call must not succeed");
    }

    private static OidcAuthException assertOidcFails(Supplier<?> call, String expectedMessage, String whatMustFail) {
        final Object returned;
        try {
            returned = call.get();
        } catch (OidcAuthException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
            return e;
        }
        // The call SUCCEEDED. Close what it handed back before failing - a construction that should have been
        // rejected must not leak past the assertion - then report the value itself, which on a token path IS
        // the credential an over-permissive check let through.
        if (returned instanceof AutoCloseable) {
            try {
                ((AutoCloseable) returned).close();
            } catch (Exception ignore) {
                // the failure below is the one that matters
            }
        }
        throw new AssertionError(whatMustFail + " [expected an OidcAuthException containing \"" + expectedMessage
                + "\", got " + returned + ']');
    }

    private static boolean awaitInside(Thread t, String method, long timeoutMillis) throws InterruptedException {
        // poll the thread's own stack until the named OidcDeviceAuth frame shows up: the only evidence that a
        // helper thread has actually ENTERED a call, as opposed to having been scheduled at all
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (isInside(t, method)) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
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

    // DiscoveryOptions permitting insecure http with a no-op prompt: tests must never print to the console
    // or try to open a real browser, which the default prompt now does. The common shape for tests reaching
    // a plaintext mock server.
    private static OidcDeviceAuth.DiscoveryOptions insecure() {
        return new OidcDeviceAuth.DiscoveryOptions().allowInsecureTransport(true).prompt(noopPrompt());
    }

    @Test(timeout = 30_000)
    public void testControlCharInUnusedTokenKindDoesNotAbortGrant() throws Exception {
        assertMemoryLeak(() -> {
            // groupsInToken=false, so signIn() serves and sends only the access_token; the id_token is
            // cached but never placed in a header or a PG-wire password. A control char in that unused id_token
            // must not reject an otherwise-usable grant - only the served kind is validated for wire safety
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                // a clean access_token (the served kind) alongside an id_token carrying a decoded control char
                return MockOidcServer.json(200, tokenJson("CLEAN-ACCESS", "bad" + jsonUnicodeEscape(0x0001) + "id", null, 3600));
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                Assert.assertEquals("CLEAN-ACCESS", auth.signIn());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testShortAllDigitStatusIsNotTreatedAsSuccess() throws Exception {
        assertMemoryLeak(() -> {
            // a real HTTP status is exactly 3 digits; a malformed 1-digit "2" (all digits, so readResponse
            // accepts it) must not be classified as a 2xx success by its leading digit and accepted as a grant
            String tokenBody = tokenJson("SHOULD-NOT-ACCEPT", null, null, 3600);
            String rawToken = "HTTP/1.1 2 OK\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Transfer-Encoding: chunked\r\n\r\n"
                    + Integer.toHexString(tokenBody.length()) + "\r\n" + tokenBody + "\r\n"
                    + "0\r\n\r\n";
            MockOidcServer.Handler handler = (method, path, body) -> {
                if (DEVICE_PATH.equals(path)) {
                    return MockOidcServer.json(200, "{"
                            + "\"device_code\":\"DEV\","
                            + "\"user_code\":\"WDJB-MJHT\","
                            + "\"verification_uri\":\"https://verify.example/device\","
                            + "\"expires_in\":300,"
                            + "\"interval\":1"
                            + "}");
                }
                return MockOidcServer.raw(rawToken);
            };
            try (MockOidcServer server = new MockOidcServer(handler);
                 OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                try {
                    auth.signIn();
                    Assert.fail("expected a malformed 1-digit status to be rejected, not accepted as success");
                } catch (OidcAuthException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(msg, msg.contains("rejected the request") || msg.contains("refusing to keep polling"));
                    Assert.assertFalse("the unaccepted token must not leak: " + msg, msg.contains("SHOULD-NOT-ACCEPT"));
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testShortAllDigitStatusNotTreatedAsTransientOrTerminal() throws Exception {
        // a real HTTP status is exactly 3 digits. A malformed 1-digit "5" must not be read as a transient 5xx
        // (which would poll on to the device-code deadline), nor a 1-digit "4" as a terminal 4xx, by the leading
        // digit alone; both fall through to the fast terminal reject rather than an infinite poll.
        for (String shortStatus : new String[]{"5", "4"}) {
            assertMemoryLeak(() -> {
                String tokenBody = tokenJson("SHOULD-NOT-ACCEPT", null, null, 3600);
                String rawToken = "HTTP/1.1 " + shortStatus + " X\r\n"
                        + "Content-Type: application/json\r\n"
                        + "Transfer-Encoding: chunked\r\n\r\n"
                        + Integer.toHexString(tokenBody.length()) + "\r\n" + tokenBody + "\r\n"
                        + "0\r\n\r\n";
                MockOidcServer.Handler handler = (method, path, body) -> {
                    if (DEVICE_PATH.equals(path)) {
                        return MockOidcServer.json(200, "{"
                                + "\"device_code\":\"DEV\","
                                + "\"user_code\":\"WDJB-MJHT\","
                                + "\"verification_uri\":\"https://verify.example/device\","
                                + "\"expires_in\":300,"
                                + "\"interval\":1"
                                + "}");
                    }
                    return MockOidcServer.raw(rawToken);
                };
                try (MockOidcServer server = new MockOidcServer(handler);
                     OidcDeviceAuth auth = newAuth(server, false, noopPrompt())) {
                    try {
                        auth.signIn();
                        Assert.fail("expected malformed 1-digit status '" + shortStatus + "' to be rejected fast");
                    } catch (OidcAuthException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(msg, msg.contains("rejected the request") || msg.contains("refusing to keep polling"));
                        // must NOT have polled to the device-code deadline (that would be a mis-classified transient)
                        Assert.assertFalse(msg, msg.contains("device code expired"));
                        Assert.assertFalse("the unaccepted token must not leak: " + msg, msg.contains("SHOULD-NOT-ACCEPT"));
                    }
                }
            });
        }
    }

    // Forces the cached access/id token to look expired WITHOUT dropping the refresh token, so the next
    // signIn()/getToken() takes the silent-refresh (or interactive re-sign-in) path. Reflection
    // because the field is private and there is no configurable clock skew to lean on anymore; the client is
    // an open module, so this reaches it without widening production visibility for the test.
    // package-private, not private: OidcDeviceAuthPersistenceTest needs the same thing and a second copy of
    // this reflection would be the third in the package. There is no non-reflective route - expires_in is
    // clamped to a default when non-positive, and the smallest usable value still leaves a live window that
    // would have to be slept out.
    static void expireCachedToken(OidcDeviceAuth auth) throws Exception {
        Field f = OidcDeviceAuth.class.getDeclaredField("expiresAtMillis");
        f.setAccessible(true);
        f.setLong(auth, 0L); // any "now" is past 0 minus the (capped, non-negative) skew, so the token reads as expired
    }

    // Reads the cached token's absolute expiry (epoch millis) so a test can assert the lifetime clamp directly.
    private static Object readField(Object instance, String name) throws Exception {
        Field f = instance.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(instance);
    }

    private static long readExpiresAtMillis(OidcDeviceAuth auth) throws Exception {
        Field f = OidcDeviceAuth.class.getDeclaredField("expiresAtMillis");
        f.setAccessible(true);
        return f.getLong(auth);
    }

    // isEndpointUnderIssuerPath is a private static security check (it scopes a /settings-advertised endpoint
    // to the pinned issuer's path); the client is an open module, so reflection reaches it without widening
    // production visibility for the test
    /**
     * Returns a description of the first field holding {@code secret}, or null when none does. Walks the
     * instance's declared fields and, one level deeper, the fields of any {@code io.questdb.client} object
     * among them - which is what reaches the sinks inside the two response parsers.
     */
    private static String findHolderOf(Object instance, String secret) throws Exception {
        for (Field f : instance.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            Object value = f.get(instance);
            if (value == null) {
                continue;
            }
            if (value instanceof StringSink && sinkContents((StringSink) value).contains(secret)) {
                return instance.getClass().getSimpleName() + '.' + f.getName();
            }
            if (value instanceof String && ((String) value).contains(secret)) {
                return instance.getClass().getSimpleName() + '.' + f.getName();
            }
            if (value != instance && value.getClass().getName().startsWith("io.questdb.client.")
                    && !(value instanceof StringSink)) {
                for (Field nested : value.getClass().getDeclaredFields()) {
                    if (Modifier.isStatic(nested.getModifiers())) {
                        continue;
                    }
                    nested.setAccessible(true);
                    Object nestedValue = nested.get(value);
                    if (nestedValue instanceof StringSink
                            && sinkContents((StringSink) nestedValue).contains(secret)) {
                        return f.getName() + '.' + nested.getName();
                    }
                    if (nestedValue instanceof String && ((String) nestedValue).contains(secret)) {
                        return f.getName() + '.' + nested.getName();
                    }
                }
            }
        }
        return null;
    }

    // isLoopbackHost is a private static security classifier (it gates the plaintext-channel MITM pin); the
    // client is an open module, so reflection reaches it without widening production visibility for the test
    private static boolean isInside(Thread t, String method) {
        for (StackTraceElement frame : t.getStackTrace()) {
            if (OidcDeviceAuth.class.getName().equals(frame.getClassName())
                    && method.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
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

    private static void parseSplitValue(int cacheSizeLimit, long address, int split, int len) throws JsonException {
        try (JsonLexer lexer = new JsonLexer(1024, cacheSizeLimit)) {
            lexer.parse(address, address + split, NOOP_JSON_PARSER);
            lexer.parse(address + split, address + len, NOOP_JSON_PARSER);
            lexer.parseLast();
        }
    }

    /**
     * The sink's WHOLE backing array as a String - past the write position too, which is where a cleared but
     * unwiped secret survives.
     */
    private static String sinkContents(StringSink sink) throws Exception {
        Field buffer = StringSink.class.getDeclaredField("buffer");
        buffer.setAccessible(true);
        return new String((char[]) buffer.get(sink));
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
