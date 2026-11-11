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

package io.questdb.client.azure.test;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ClientAuthenticationException;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.identity.ClientSecretCredentialBuilder;
import io.questdb.client.azure.AzureTokenSource;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises the pinned Azure Identity/MSAL service-principal path with a controlled HTTP transport and the
 * SDK's default retry policy. No request reaches a real identity endpoint.
 */
public class AzureClientSecretCredentialLibraryTest {
    private static final String AUTHORITY = "https://login.microsoftonline.com/" + "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    private static final String BODY_SENTINEL = "BODY-SENTINEL";
    private static final String CLIENT_SECRET = "not-a-real-secret";

    @Test(timeout = 30_000)
    public void testClientRejectionsWithoutKnownAadstsCodesArePermanent() {
        for (int status : new int[]{400, 401}) {
            assertPermanentClientRejection(status, false);
        }
    }

    @Test(timeout = 30_000)
    public void testFinalRetryAfterSurvivesSdkRetries() {
        assertRetryAfter(new TokenEndpoint(429, "7"), false, 7_000);
    }

    @Test(timeout = 30_000)
    public void testMalformedResponseBodyNeverReachesFailureDiagnostics() {
        TokenEndpoint endpoint = new TokenEndpoint(400, null, "{\"unexpected\":\"" + BODY_SENTINEL + "\"}");
        AzureTokenSource source = new AzureTokenSource(credential(endpoint), "api://qdb");
        try {
            source.fetchToken();
            Assert.fail("expected the malformed token response to fail");
        } catch (TokenUnavailableException e) {
            assertNoResponseBody(e);
        }
        Assert.assertEquals(1, endpoint.tokenRequests.get());

        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .coldWaitMillis(10_000).backoffInitialMillis(60_000).backoffMaxMillis(60_000).build()) {
            try {
                provider.getToken();
                Assert.fail("expected the provider to report the source failure");
            } catch (TokenUnavailableException e) {
                assertNoResponseBody(e);
            }
            Assert.assertNotNull(provider.getLastFailure());
            Assert.assertFalse(provider.getLastFailure().getMessage(),
                    provider.getLastFailure().getMessage().contains(BODY_SENTINEL));
            Assert.assertEquals(2, endpoint.tokenRequests.get());
        }
    }

    @Test(timeout = 30_000)
    public void testMissingOrMalformedRetryAfterUsesProviderBackoff() {
        for (String retryAfter : Arrays.asList(null, "", "not-a-delay", "Wed, 21 Oct 2015 07:28:00 GMT", "-1")) {
            assertRetryAfter(new TokenEndpoint(429, retryAfter), false, TokenUnavailableException.NO_RETRY_AFTER);
        }
    }

    @Test(timeout = 30_000)
    public void testOAuthErrorDescriptionNeverReachesFailureDiagnostics() {
        TokenEndpoint endpoint = new TokenEndpoint(400, null,
                "{\"error\":\"invalid_client\",\"error_description\":\"AADSTS7000215: " + BODY_SENTINEL + "\"}");
        try {
            new AzureTokenSource(credential(endpoint), "api://qdb").fetchToken();
            Assert.fail("expected Entra to reject the credential");
        } catch (TokenUnavailableException e) {
            assertNoResponseBody(e);
            Assert.assertFalse(e.getMessage(), e.isRetryable());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("AADSTS7000215"));
        }
        Assert.assertEquals(1, endpoint.tokenRequests.get());
    }

    @Test(timeout = 30_000)
    public void testServerFailureRetainsRetryAfter() {
        assertRetryAfter(new TokenEndpoint(503, "7"), false, 7_000);
    }

    @Test(timeout = 30_000)
    public void testWrappedClientRejectionsWithoutKnownAadstsCodesArePermanent() {
        for (int status : new int[]{400, 401}) {
            assertPermanentClientRejection(status, true);
        }
    }

    @Test(timeout = 30_000)
    public void testWrappedMsalFailureRetainsRetryAfter() {
        assertRetryAfter(new TokenEndpoint(429, " 7 "), true, 7_000);
    }

    private static void assertNoResponseBody(TokenUnavailableException e) {
        Assert.assertFalse(e.getMessage(), e.getMessage().contains(BODY_SENTINEL));
        Assert.assertFalse(e.getMessage(), e.getMessage().contains(CLIENT_SECRET));
        Assert.assertNull("never retain an SDK exception that references the raw request", e.getCause());
    }

    private static void assertPermanentClientRejection(int status, boolean isWrapped) {
        // Entra reports an invalid application ID as AADSTS700038, outside the known-code allowlist.
        TokenEndpoint endpoint = new TokenEndpoint(status, null,
                "{\"error\":\"unauthorized_client\",\"error_description\":\"AADSTS700038: "
                        + "00000000-0000-0000-0000-000000000000 is not a valid application identifier. "
                        + BODY_SENTINEL + "\"}");
        try {
            new AzureTokenSource(credential(endpoint, isWrapped), "api://qdb").fetchToken();
            Assert.fail("expected Entra to reject the application");
        } catch (TokenUnavailableException e) {
            Assert.assertFalse(e.getMessage(), e.isRetryable());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("HTTP " + status));
            Assert.assertEquals(TokenUnavailableException.NO_RETRY_AFTER, e.getRetryAfterMillis());
            assertNoResponseBody(e);
        }
        Assert.assertEquals("a configuration rejection must not trigger SDK retries", 1, endpoint.tokenRequests.get());
    }

    private static void assertRetryAfter(TokenEndpoint endpoint, boolean isWrapped, long expectedRetryAfterMillis) {
        try {
            new AzureTokenSource(credential(endpoint, isWrapped), "api://qdb").fetchToken();
            Assert.fail("expected the token endpoint to reject the request");
        } catch (TokenUnavailableException e) {
            Assert.assertEquals("the SDK must exhaust its default three retries", 4, endpoint.tokenRequests.get());
            Assert.assertTrue(e.getMessage(), e.isRetryable());
            Assert.assertEquals(e.getMessage(), expectedRetryAfterMillis, e.getRetryAfterMillis());
            Assert.assertNull("never retain an SDK exception that references the raw request", e.getCause());
            Assert.assertFalse(e.getMessage(), e.getMessage().contains(CLIENT_SECRET));
        }
    }

    private static TokenCredential credential(TokenEndpoint endpoint) {
        return new ClientSecretCredentialBuilder()
                .tenantId("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                .clientId("11111111-2222-3333-4444-555555555555")
                .clientSecret(CLIENT_SECRET)
                .disableInstanceDiscovery()
                .httpClient(endpoint)
                .build();
    }

    private static TokenCredential credential(TokenEndpoint endpoint, boolean isWrapped) {
        TokenCredential credential = credential(endpoint);
        return isWrapped ? context -> credential.getToken(context).onErrorMap(
                failure -> new ClientAuthenticationException("authentication failed", null, failure)) : credential;
    }

    private static final class StubResponse extends HttpResponse {
        private final byte[] body;
        private final HttpHeaders headers;
        private final int status;

        private StubResponse(HttpRequest request, int status, String body, String retryAfter) {
            super(request);
            this.body = body.getBytes(StandardCharsets.UTF_8);
            this.headers = new HttpHeaders().set("Content-Type", "application/json");
            this.status = status;
            if (retryAfter != null) {
                headers.set("rEtRy-AfTeR", retryAfter);
            }
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return Flux.just(ByteBuffer.wrap(body));
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.just(body);
        }

        @Override
        public Mono<String> getBodyAsString() {
            return getBodyAsString(StandardCharsets.UTF_8);
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return Mono.just(new String(body, charset));
        }

        @Override
        public String getHeaderValue(String name) {
            return headers.getValue(name);
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public int getStatusCode() {
            return status;
        }
    }

    private static final class TokenEndpoint implements HttpClient {
        private final String body;
        private final String retryAfter;
        private final int status;
        private final AtomicInteger tokenRequests = new AtomicInteger();

        private TokenEndpoint(int status, String retryAfter) {
            this(status, retryAfter, "{\"error\":\"temporarily_unavailable\","
                    + "\"error_description\":\"The endpoint is throttling this application\"}");
        }

        private TokenEndpoint(int status, String retryAfter, String body) {
            this.body = body;
            this.retryAfter = retryAfter;
            this.status = status;
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            if (request.getHttpMethod() == HttpMethod.GET) {
                return Mono.just(new StubResponse(request, 200,
                        "{\"authorization_endpoint\":\"" + AUTHORITY + "/oauth2/v2.0/authorize\","
                                + "\"token_endpoint\":\"" + AUTHORITY + "/oauth2/v2.0/token\","
                                + "\"issuer\":\"" + AUTHORITY + "/v2.0\","
                                + "\"jwks_uri\":\"" + AUTHORITY + "/discovery/v2.0/keys\"}", null));
            }
            Assert.assertEquals(HttpMethod.POST, request.getHttpMethod());
            // Keep the SDK's first three retries immediate; only its final response supplies the provider's hint.
            String delay = tokenRequests.incrementAndGet() <= 3 ? "0" : retryAfter;
            return Mono.just(new StubResponse(request, status, body, delay));
        }
    }
}
